open OUnit2
open Zarr
open Zarr.Indexing
open Zarr.Codecs
open Zarr_eio.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: Node.Array.t list * Node.Group.t list]
let print_int_array = [%show : int array]

module type EIO_STORE = sig
  include Zarr.Storage.STORE with type 'a Deferred.t = 'a
end

let test_storage
  (type a) (module M : EIO_STORE with type t = a) (store : a) =
  let open M in
  let gnode = Node.Group.root in

  let nodes = hierarchy store in
  assert_equal ~printer:print_node_pair ([], []) nodes;

  Group.create store gnode;
  let exists = Group.exists store gnode in
  assert_equal ~printer:string_of_bool true exists;

  let meta = Group.metadata store gnode in
  assert_equal ~printer:Metadata.Group.show Metadata.Group.default meta;

  Group.delete store gnode;
  let exists = Group.exists store gnode in
  assert_equal ~printer:string_of_bool false exists;
  let nodes = hierarchy store in
  assert_equal ~printer:print_node_pair ([], []) nodes;

  let attrs = `Assoc [("questions", `String "answer")] in
  Group.create ~attrs store gnode;
  let meta = Group.metadata store gnode in
  assert_equal ~printer:Yojson.Safe.show attrs @@ Metadata.Group.attributes meta;

  let exists = Array.exists store @@ Node.Array.(gnode / "non-member") in
  assert_equal ~printer:string_of_bool false exists;

  let cfg =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = End
    ;index_codecs = [`Bytes BE]
    ;codecs = [`Bytes LE]} in
  let anode = Node.Array.(gnode / "arrnode") in
  let slice = [|R [|0; 20|]; I 10; R [|0; 29|]|] in
  let exp = Ndarray.init Complex32 [|21; 1; 30|] (Fun.const Complex.one) in

  List.iter
    (fun codecs ->
      Array.create
        ~codecs ~shape:[|100; 100; 50|] ~chunks:[|10; 15; 20|]
        Complex32 Complex.one anode store;
      Array.write store anode slice exp;
      let got = Array.read store anode slice Complex32 in
      assert_equal exp got;
      Ndarray.fill exp Complex.{re=2.0; im=0.};
      Array.write store anode slice exp;
      let got = Array.read store anode slice Complex32 in
      assert_equal exp got;
      Ndarray.fill exp Complex.{re=0.; im=3.0};
      Array.write store anode slice exp;
      let got = Array.read store anode slice Complex32 in
      assert_equal exp got)
    [[`ShardingIndexed cfg]; [`Bytes BE]];

  let child = Node.Group.of_path "/some/child/group" in
  Group.create store child;
  let arrays, groups = Group.children store gnode in
  assert_equal
    ~printer:string_of_list ["/arrnode"] (List.map Node.Array.to_path arrays);
  assert_equal
    ~printer:string_of_list ["/some"] (List.map Node.Group.to_path groups);

  let c = Group.children store @@ Node.Group.(root / "fakegroup") in
  assert_equal ([], []) c;

  let ac, gc = hierarchy store in
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/arrnode"; "/some"; "/some/child"; "/some/child/group"] got;

  (* tests for renaming nodes *)
  let some = Node.Group.of_path "/some/child" in
  Array.rename store anode "ARRAYNODE";
  Group.rename store some "CHILD";
  let ac, gc = hierarchy store in
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/ARRAYNODE"; "/some"; "/some/CHILD"; "/some/CHILD/group"] got;
  (* restore old array node name. *)
  Array.rename store (Node.Array.of_path "/ARRAYNODE") "arrnode";

  let nshape = [|25; 32; 10|] in
  Array.reshape store anode nshape;
  let meta = Array.metadata store anode in
  assert_equal ~printer:print_int_array nshape @@ Metadata.Array.shape meta;
  assert_raises
    (Zarr.Storage.Key_not_found "fakegroup/zarr.json")
    (fun () -> Array.metadata store Node.Array.(gnode / "fakegroup"));

  Array.delete store anode;
  clear store;
  let got = hierarchy store in
  assert_equal ~printer:print_node_pair ([], []) got

let _ =
  run_test_tt_main @@ ("Run Zarr Eio API tests" >::: [
    "test eio-based stores" >::
    (fun _ ->
      Eio_main.run @@ fun env ->
      let rand_num = string_of_int @@ Random.int 10_000 in
      let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
      let s = FilesystemStore.create ~env tmp_dir in

      assert_raises
        (Sys_error (Format.sprintf "%s: File exists" tmp_dir))
        (fun () -> FilesystemStore.create ~env tmp_dir);

      (* ensure it works with an extra "/" appended to directory name. *)
      ignore @@ FilesystemStore.open_store ~env (tmp_dir ^ "/");

      let fakedir = "non-existant-zarr-store112345.zarr" in
      assert_raises
        (Sys_error (Printf.sprintf "%s: No such file or directory" fakedir))
        (fun () -> FilesystemStore.open_store ~env fakedir);

      let fn = Filename.temp_file "nonexistantfile" ".zarr" in
      assert_raises
        (Zarr.Storage.Not_a_filesystem_store fn)
        (fun () -> FilesystemStore.open_store ~env fn);

      test_storage (module MemoryStore) @@ MemoryStore.create ();
      test_storage (module FilesystemStore) s)
  ])
