open OUnit2
open Zarr
open Zarr.Indexing
open Zarr.Codecs
open Zarr_sync.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: Node.Array.t list * Node.Group.t list]

module type SYNC_STORE = Zarr.Storage.S with type 'a io := 'a

let test_storage
  (type a) (module M : SYNC_STORE with type t = a) (store : a) =
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
    {chunk_shape = [2; 5; 5]
    ;index_location = End
    ;index_codecs = [`Bytes LE; `Crc32c]
    ;codecs = [`Transpose [2; 0; 1]; `Bytes BE; `Zstd (0, false)]} in
  let cfg2 =
    {chunk_shape = [2; 5; 5]
    ;index_location = Start
    ;index_codecs = [`Bytes BE]
    ;codecs = [`Bytes LE]} in
  let anode = Node.Array.(gnode / "arrnode") in
  let slice = [R (0, 20); I 10; R (0, 29)] in
  let bigger_slice =  [R (0, 21); L [9; 10] ; R (0, 30)] in

  List.iter
    (fun codecs ->
      Array.create ~codecs ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Complex32 Complex.one anode store;
      let exp = Ndarray.init Complex32 [21; 1; 30] (Fun.const Complex.one) in
      let got = Array.read store anode slice Complex32 in
      assert_equal exp got;
      Ndarray.fill exp Complex.{re=2.0; im=0.};
      Array.write store anode slice exp;
      let got = Array.read store anode slice Complex32 in
      (* test if a bigger slice containing new elements can be read from store *)
      let _ = Array.read store anode bigger_slice Complex32 in
      assert_equal exp got;
      (* test writing a bigger slice to store *)
      Array.write store anode bigger_slice @@ Ndarray.init Complex32 [22; 2; 31] (Fun.const Complex.{re=0.; im=3.0});
      let got = Array.read store anode slice Complex32 in
      Ndarray.fill exp Complex.{re=0.; im=3.0};
      assert_equal exp got;
      Array.delete store anode)
    [[`ShardingIndexed cfg]; [`ShardingIndexed cfg2]];

  (* repeat tests for non-sharding codec chain *)
  Array.create ~sep:`Dot ~codecs:[`Bytes BE] ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Ndarray.Int Int.max_int anode store;
  (* test path where there is no chunk key present in store *)
  let exp = Ndarray.init Int [21; 1; 30] (Fun.const Int.max_int) in
  Array.write store anode slice exp;
  let got = Array.read store anode slice Int in
  assert_equal exp got;
  (* test path where there is a chunk key present in store at write time. *)
  Array.write store anode slice exp;
  let got = Array.read store anode slice Int in
  assert_equal exp got;

  assert_raises
    (Zarr.Storage.Invalid_data_type)
    (fun () -> Array.read store anode slice Ndarray.Char);
  let badslice = [R (0, 20); I 10; F; F] in
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> Array.read store anode badslice Ndarray.Int);
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> Array.write store anode badslice exp);
  assert_raises
    (Zarr.Storage.Invalid_array_slice)
    (fun () -> Array.write store anode [R (0, 20); F; F] exp);
  let badarray = Ndarray.init Float64 [21; 1; 30] (Fun.const 0.) in
  assert_raises
    (Zarr.Storage.Invalid_data_type)
    (fun () -> Array.write store anode slice badarray);

  let child = Node.Group.of_path "/some/child/group" in
  Group.create store child;
  let arrays, groups = Group.children store gnode in
  assert_equal
    ~printer:string_of_list ["/arrnode"] (List.map Node.Array.to_path arrays);
  assert_equal
    ~printer:string_of_list ["/some"] (List.map Node.Group.to_path groups);

  assert_equal ([], []) @@ Group.children store child;
  assert_equal ([], []) @@ Group.children store Node.Group.(root / "fakegroup");

  let ac, gc = hierarchy store in
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/arrnode"; "/some"; "/some/child"; "/some/child/group"] got;

  (* tests for renaming nodes *)
  let some = Node.Group.of_path "/some/child" in
  Group.rename store some "CHILD";
  Array.rename store anode "ARRAYNODE";
  let ac, gc = hierarchy store in
  let got =
    List.fast_sort String.compare @@
    List.map Node.Array.show ac @ List.map Node.Group.show gc in
  assert_equal
    ~printer:string_of_list
    ["/"; "/ARRAYNODE"; "/some"; "/some/CHILD"; "/some/CHILD/group"] got;
  assert_raises
    (Zarr.Storage.Key_not_found "fakegroup")
    (fun () -> Group.rename store Node.Group.(gnode / "fakegroup") "somename");
  assert_raises
    (Zarr.Storage.Key_not_found "fakearray")
    (fun () -> Array.rename store Node.Array.(gnode / "fakearray") "somename");

  (* restore old array node name. *)
  Array.rename store (Node.Array.of_path "/ARRAYNODE") "arrnode";
  let nshape = [25; 32; 10] in
  Array.reshape store anode nshape;
  let meta = Array.metadata store anode in
  assert_equal ~printer:[%show : int list] nshape @@ Metadata.Array.shape meta;
  assert_raises
    (Zarr.Storage.Invalid_resize_shape)
    (fun () -> Array.reshape store anode [25; 10]);
  assert_raises
    (Zarr.Storage.Key_not_found "fakegroup/zarr.json")
    (fun () -> Array.metadata store Node.Array.(gnode / "fakegroup"));

  Array.delete store anode;
  clear store;
  let got = hierarchy store in
  assert_equal ~printer:print_node_pair ([], []) got

let _ =
  run_test_tt_main @@ ("Run Zarr sync API tests" >::: [
  "test sync-based stores" >::
    (fun _ ->
      let rand_num = string_of_int @@ Random.int 1_000_000 in
      let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
      let s = FilesystemStore.create tmp_dir in

      assert_raises
        (Sys_error (Format.sprintf "%s: File exists" tmp_dir))
        (fun () -> FilesystemStore.create tmp_dir);

      (* inject a bad metadata document to test correct parsing of bad child
         nodes when discovering children of a group. *)
      let dname = tmp_dir ^ "/badnode" in
      let fname = Filename.concat dname "zarr.json" in
      Sys.mkdir dname 0o700;
      Out_channel.with_open_bin
        fname
        (Fun.flip Out_channel.output_string {|{"zarr_format":3,"node_type":"unknown"}|});
      assert_raises
        (Zarr.Metadata.Parse_error "invalid node_type in badnode/zarr.json")
        (fun () -> FilesystemStore.hierarchy s);
      Sys.(remove fname; rmdir dname);

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

      (* test with non-existant archive *)
      let zpath = tmp_dir ^ ".zip" in
      ZipStore.with_open `Read_write zpath (fun z -> test_storage (module ZipStore) z);
      (* test just opening the now exisitant archive created by the previous test. *)
      ZipStore.with_open `Read_only zpath (fun _ -> ());
      test_storage (module MemoryStore) @@ MemoryStore.create ();
      test_storage (module FilesystemStore) s)
  ])
