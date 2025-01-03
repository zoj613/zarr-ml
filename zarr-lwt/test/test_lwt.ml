open OUnit2
open Zarr
open Zarr.Indexing
open Zarr.Codecs
open Zarr_lwt.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: Node.Array.t list * Node.Group.t list]

module type LWT_STORE = Zarr.Storage.S with type 'a io := 'a Lwt.t

let test_storage
  (type a) (module M : LWT_STORE with type t = a) (store : a) =
  let open M in
  let open IO.Infix in
  let gnode = Node.Group.root in

  hierarchy store >>= fun nodes ->
  assert_equal ~printer:print_node_pair ([], []) nodes;

  Group.create store gnode >>= fun () ->
  Group.exists store gnode >>= fun exists ->
  assert_equal ~printer:string_of_bool true exists;

  Group.metadata store gnode >>= fun meta ->
  assert_equal ~printer:Metadata.Group.show Metadata.Group.default meta;

  Group.delete store gnode >>= fun () ->
  Group.exists store gnode >>= fun exists ->
  assert_equal ~printer:string_of_bool false exists;
  hierarchy store >>= fun nodes ->
  assert_equal ~printer:print_node_pair ([], []) nodes;

  let attrs = `Assoc [("questions", `String "answer")] in
  Group.create ~attrs store gnode >>= fun () ->
  Group.metadata store gnode >>= fun meta ->
  assert_equal ~printer:Yojson.Safe.show attrs @@ Metadata.Group.attributes meta;

  Array.exists store @@ Node.Array.(gnode / "non-member") >>= fun exists ->
  assert_equal ~printer:string_of_bool false exists;

  let cfg =
    {chunk_shape = [2; 5; 5]
    ;index_location = End
    ;index_codecs = [`Bytes BE]
    ;codecs = [`Bytes LE]} in
  let anode = Node.Array.(gnode / "arrnode") in
  let slice = [R (0, 20); I 10; R (0, 29)] in
  let exp = Ndarray.init Ndarray.Complex32 [21; 1; 30] (Fun.const Complex.one) in

  Lwt_list.iter_s
    (fun codecs ->
      Array.create
        ~codecs ~shape:[100; 100; 50] ~chunks:[10; 15; 20]
        Ndarray.Complex32 Complex.one anode store >>= fun () ->
      Array.write store anode slice exp >>= fun () ->
      Array.read store anode slice Complex32 >>= fun got ->
      assert_equal exp got;
      Ndarray.fill exp Complex.{re=2.0; im=0.};
      Array.write store anode slice exp >>= fun () ->
      Array.read store anode slice Complex32 >>= fun arr ->
      assert_equal exp arr;
      Ndarray.fill exp Complex.{re=0.; im=3.0};
      Array.write store anode slice exp >>= fun () ->
      Array.read store anode slice Complex32 >>= fun got ->
      assert_equal exp got;
      match codecs with
      | [`ShardingIndexed _] -> Array.delete store anode
      | _ -> IO.return_unit)
    [[`ShardingIndexed cfg]; [`Bytes BE]] >>= fun () ->

  let child = Node.Group.of_path "/some/child/group" in
  Group.create store child >>= fun () ->
  Group.children store gnode >>= fun (arrays, groups) ->
  assert_equal
    ~printer:string_of_list ["/arrnode"] (List.map Node.Array.to_path arrays);
  assert_equal
    ~printer:string_of_list ["/some"] (List.map Node.Group.to_path groups);

  Group.children store @@ Node.Group.(root / "fakegroup") >>= fun c ->
  assert_equal ([], []) c;

  hierarchy store >>= fun (ac, gc) ->
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/arrnode"; "/some"; "/some/child"; "/some/child/group"] got;

  (* tests for renaming nodes *)
  let some = Node.Group.of_path "/some/child" in
  Array.rename store anode "ARRAYNODE" >>= fun () ->
  Group.rename store some "CHILD" >>= fun () ->
  hierarchy store >>= fun (ac, gc) ->
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/ARRAYNODE"; "/some"; "/some/CHILD"; "/some/CHILD/group"] got;
  (* restore old array node name. *)
  Array.rename store (Node.Array.of_path "/ARRAYNODE") "arrnode" >>= fun () ->

  let nshape = [25; 32; 10] in
  Array.reshape store anode nshape >>= fun () ->
  Array.metadata store anode >>= fun meta ->
  assert_equal ~printer:[%show : int list] nshape @@ Metadata.Array.shape meta;

  Array.delete store anode >>= fun () ->
  clear store >>= fun () ->
  hierarchy store >>= fun got ->
  assert_equal ~printer:print_node_pair ([], []) got;
  IO.return_unit

let _ = 
  run_test_tt_main @@ ("Run Zarr Lwt API tests" >::: [
    "test lwt-based stores" >::
    (fun _ ->
      let rand_num = string_of_int @@ Random.int 100_000 in
      let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
      let s = FilesystemStore.create tmp_dir in

      assert_raises
        (Sys_error (Format.sprintf "%s: File exists" tmp_dir))
        (fun () -> FilesystemStore.create tmp_dir);

      (* ensure it works with an extra "/" appended to directory name. *)
      ignore @@ FilesystemStore.open_store (tmp_dir ^ "/");

      let fakedir = "non-existant-zarr-store112345.zarr" in
      assert_raises
        (Sys_error (Printf.sprintf "%s: No such file or directory" fakedir))
        (fun () -> FilesystemStore.open_store fakedir);

      let fn = Filename.temp_file "nonexistantfile" ".zarr" in
      assert_raises
        (Zarr.Storage.Not_a_filesystem_store fn)
        (fun () -> FilesystemStore.open_store fn);

      (* ZipStore configuration *)
      let zpath = tmp_dir ^ ".zip" in
      (* AmazonS3Store configuration *)
      let region = Aws_s3.Region.minio ~port:9000 ~host:"localhost" ()
      and bucket = "test-bucket-lwt"
      and profile = "default" in

      Lwt_main.run @@ Lwt.join 
        [ZipStore.with_open `Read_write zpath (fun z -> test_storage (module ZipStore) z)
         (* test just opening the now exisitant archive created by the previous test. *)
        ;ZipStore.with_open `Read_only zpath (fun _ -> Lwt.return_unit)
        ;AmazonS3Store.with_open ~region ~bucket ~profile (test_storage (module AmazonS3Store))
        ;test_storage (module MemoryStore) @@ MemoryStore.create ()
        ;test_storage (module FilesystemStore) s])
])
