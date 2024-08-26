open Bigarray
open OUnit2
open Zarr.Metadata
open Zarr.Node
open Zarr.Codecs
open Zarr_eio.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: ArrayNode.t list * GroupNode.t list]
let print_int_array = [%show : int array]

module type SYNC_STORE = sig
  include Zarr.Storage.STORE with type 'a Deferred.t = 'a
end

let test_storage
  (type a) (module M : SYNC_STORE with type t = a) (store : a) =
  let open M in
  let gnode = GroupNode.root in

  let nodes = find_all_nodes store in
  assert_equal ~printer:print_node_pair ([], []) nodes;

  create_group store gnode;
  let exists = group_exists store gnode in
  assert_equal ~printer:string_of_bool true exists;

  let meta = group_metadata store gnode in
  assert_equal ~printer:GroupMetadata.show GroupMetadata.default meta;

  erase_group_node store gnode;
  let exists = group_exists store gnode in
  assert_equal ~printer:string_of_bool false exists;
  let nodes = find_all_nodes store in
  assert_equal ~printer:print_node_pair ([], []) nodes;

  let attrs = `Assoc [("questions", `String "answer")] in
  create_group ~attrs store gnode;
  let meta = group_metadata store gnode in
  assert_equal ~printer:Yojson.Safe.show attrs @@ GroupMetadata.attributes meta;

  let exists = array_exists store @@ ArrayNode.(gnode / "non-member") in
  assert_equal ~printer:string_of_bool false exists;

  let cfg =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = End
    ;index_codecs = [`Bytes LE; `Crc32c]
    ;codecs = [`Transpose [|2; 0; 1|]; `Bytes BE; `Gzip L5]} in
  let cfg2 =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = Start
    ;index_codecs = [`Bytes BE]
    ;codecs = [`Bytes LE]} in
  let anode = ArrayNode.(gnode / "arrnode") in
  let slice = Owl_types.[|R [0; 20]; I 10; R [0; 29]|] in

  List.iter
    (fun codecs ->
      create_array
        ~codecs ~shape:[|100; 100; 50|] ~chunks:[|10; 15; 20|]
        Complex32 Complex.one anode store;
      let exp = Genarray.init Complex32 C_layout [|21; 1; 30|] (Fun.const Complex.one) in
      let got = read_array store anode slice Complex32 in
      assert_equal ~printer:Owl_pretty.dsnda_to_string exp got;
      Genarray.fill exp Complex.{re=2.0; im=0.};
      write_array store anode slice exp;
      let got = read_array store anode slice Complex32 in
      assert_equal ~printer:Owl_pretty.dsnda_to_string exp got;
      Genarray.fill exp Complex.{re=0.; im=3.0};
      write_array store anode slice exp;
      let got = read_array store anode slice Complex32 in
      assert_equal ~printer:Owl_pretty.dsnda_to_string exp got;
      erase_array_node store anode)
    [[`ShardingIndexed cfg]; [`ShardingIndexed cfg2]];

  (* repeat tests for non-sharding codec chain *)
  create_array
    ~sep:`Dot ~codecs:[`Bytes BE]
    ~shape:[|100; 100; 50|] ~chunks:[|10; 15; 20|]
    Bigarray.Int Int.max_int anode store;
  (* test path where there is no chunk key present in store *)
  let exp = Genarray.init Int C_layout [|21; 1; 30|] (Fun.const Int.max_int) in
  write_array store anode slice exp;
  let got = read_array store anode slice Int in
  assert_equal ~printer:Owl_pretty.dsnda_to_string exp got;
  (* test path where there is a chunk key present in store at write time. *)
  write_array store anode slice exp;
  let got = read_array store anode slice Int in
  assert_equal ~printer:Owl_pretty.dsnda_to_string exp got;

  assert_raises
    (Zarr.Storage.Invalid_data_type)
    (fun () -> read_array store anode slice Bigarray.Char);
  let badslice = Owl_types.[|R [0; 20]; I 10; R []; R [] |] in
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> read_array store anode badslice Bigarray.Int);
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> write_array store anode badslice exp);
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> write_array store anode Owl_types.[|R [0; 20]; R []; R []|] exp);
  let badarray = Genarray.init Float64 C_layout [|21; 1; 30|] (Fun.const 0.) in
  assert_raises
    (Zarr.Storage.Invalid_data_type)
    (fun () -> write_array store anode slice badarray);

  let child = GroupNode.of_path "/some/child" in
  create_group store child;
  let arrays, groups = find_child_nodes store gnode in
  assert_equal
    ~printer:string_of_list ["/arrnode"] (List.map ArrayNode.to_path arrays);
  assert_equal
    ~printer:string_of_list ["/some"] (List.map GroupNode.to_path groups);

  let c = find_child_nodes store @@ GroupNode.(root / "fakegroup") in
  assert_equal ([], []) c;

  let ac, gc = find_all_nodes store in
  let got =
    List.fast_sort String.compare @@
    List.map ArrayNode.show ac @ List.map GroupNode.show gc in
  assert_equal
    ~printer:string_of_list ["/"; "/arrnode"; "/some"; "/some/child"] got;

  let nshape = [|25; 32; 10|] in
  reshape store anode nshape;
  let meta = array_metadata store anode in
  assert_equal ~printer:print_int_array nshape @@ ArrayMetadata.shape meta;
  assert_raises
    (Zarr.Storage.Invalid_resize_shape)
    (fun () -> reshape store anode [|25; 10|]);
  assert_raises
    (Zarr.Storage.Key_not_found "fakegroup/zarr.json")
    (fun () -> array_metadata store ArrayNode.(gnode / "fakegroup"));

  erase_array_node store anode;
  erase_all_nodes store;
  let got = find_all_nodes store in
  assert_equal ~printer:print_node_pair ([], []) got

let _ =
  run_test_tt_main @@ ("Run Zarr Eio API tests" >::: [
    "test eio-based stores" >::
    (fun _ ->
      Eio_main.run @@ fun env ->
      let rand_num = string_of_int @@ Random.int 1_000_000 in
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

      test_storage (module FilesystemStore) s)
  ])
