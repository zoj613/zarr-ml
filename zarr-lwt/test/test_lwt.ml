open OUnit2
open Zarr.Metadata
open Zarr.Node
open Zarr.Codecs
open Zarr_lwt.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: ArrayNode.t list * GroupNode.t list]
let print_int_array = [%show : int array]

module type SYNC_STORE = sig
  include Zarr.Storage.STORE with type 'a Deferred.t = 'a Lwt.t
end

let test_storage
  (type a) (module M : SYNC_STORE with type t = a) (store : a) =
  let open M in
  let open M.Deferred.Infix in
  let gnode = GroupNode.root in

  find_all_nodes store >>= fun nodes ->
  assert_equal ~printer:print_node_pair ([], []) nodes;

  create_group store gnode >>= fun () ->
  group_exists store gnode >>= fun exists ->
  assert_equal ~printer:string_of_bool true exists;

  group_metadata store gnode >>= fun meta ->
  assert_equal ~printer:GroupMetadata.show GroupMetadata.default meta;

  erase_group_node store gnode >>= fun () ->
  group_exists store gnode >>= fun exists ->
  assert_equal ~printer:string_of_bool false exists;
  find_all_nodes store >>= fun nodes ->
  assert_equal ~printer:print_node_pair ([], []) nodes;

  let attrs = `Assoc [("questions", `String "answer")] in
  create_group ~attrs store gnode >>= fun () ->
  group_metadata store gnode >>= fun meta ->
  assert_equal ~printer:Yojson.Safe.show attrs @@ GroupMetadata.attributes meta;

  array_exists store @@ ArrayNode.(gnode / "non-member") >>= fun exists ->
  assert_equal ~printer:string_of_bool false exists;

  let cfg =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = End
    ;index_codecs = [`Bytes BE]
    ;codecs = [`Bytes LE]} in
  let anode = ArrayNode.(gnode / "arrnode") in
  let slice = Owl_types.[|R [0; 20]; I 10; R [0; 29]|] in

  Lwt_list.iter_s
    (fun codecs ->
      let open Bigarray in
      create_array
        ~codecs ~shape:[|100; 100; 50|] ~chunks:[|10; 15; 20|]
        Complex32 Complex.one anode store >>= fun () ->
      let exp = Genarray.init Complex32 C_layout [|21; 1; 30|] (Fun.const Complex.one) in
      write_array store anode slice exp >>= fun () ->
      read_array store anode slice Complex32 >>= fun got ->
      assert_equal ~printer:Owl_pretty.dsnda_to_string exp got;
      Genarray.fill exp Complex.{re=2.0; im=0.};
      write_array store anode slice exp >>= fun () ->
      read_array store anode slice Complex32 >>= fun arr ->
      assert_equal ~printer:Owl_pretty.dsnda_to_string exp arr;
      Genarray.fill exp Complex.{re=0.; im=3.0};
      write_array store anode slice exp >>= fun () ->
      read_array store anode slice Complex32 >>| fun got ->
      assert_equal ~printer:Owl_pretty.dsnda_to_string exp got)
    [[`ShardingIndexed cfg]; [`Bytes BE]] >>= fun () ->

  let child = GroupNode.of_path "/some/child" in
  create_group store child >>= fun () ->
  find_child_nodes store gnode >>= fun (arrays, groups) ->
  assert_equal
    ~printer:string_of_list ["/arrnode"] (List.map ArrayNode.to_path arrays);
  assert_equal
    ~printer:string_of_list ["/some"] (List.map GroupNode.to_path groups);

  find_child_nodes store @@ GroupNode.(root / "fakegroup") >>= fun c ->
  assert_equal ([], []) c;

  find_all_nodes store >>= fun (ac, gc) ->
  let got =
    List.fast_sort String.compare @@
    List.map ArrayNode.show ac @ List.map GroupNode.show gc in
  assert_equal
    ~printer:string_of_list ["/"; "/arrnode"; "/some"; "/some/child"] got;

  let nshape = [|25; 32; 10|] in
  reshape store anode nshape >>= fun () ->
  array_metadata store anode >>= fun meta ->
  assert_equal ~printer:print_int_array nshape @@ ArrayMetadata.shape meta;

  erase_array_node store anode >>= fun () ->
  erase_all_nodes store >>= fun () ->
  find_all_nodes store >>= fun got ->
  assert_equal ~printer:print_node_pair ([], []) got;
  Deferred.return_unit

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

      ignore @@ FilesystemStore.open_store tmp_dir;

      let fakedir = "non-existant-zarr-store112345.zarr" in
      assert_raises
        (Sys_error (Printf.sprintf "%s: No such file or directory" fakedir))
        (fun () -> FilesystemStore.open_store fakedir);

      let fn = Filename.temp_file "nonexistantfile" ".zarr" in
      assert_raises
        (Zarr.Storage.Not_a_filesystem_store fn)
        (fun () -> FilesystemStore.open_store fn);

      Lwt_main.run @@
      Lwt.join 
        [test_storage (module MemoryStore) @@ MemoryStore.create ()
        ;test_storage (module FilesystemStore) s])
])
