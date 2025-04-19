open OUnit2
open Zarr
open Zarr.Indexing
open Zarr.Codecs
open Zarr_sync.Storage

let string_of_list = [%show: string list]
let print_node_pair = [%show: Node.Array.t list * Node.Group.t list]
let fold_result r = Result.fold ~ok:Fun.id ~error:(fun _ -> assert_failure "This should not fail.") r

module Make (E: sig type t end) (M : Storage.S with type 'a io := 'a and type error := E.t) = struct
  open M
  open IO.Syntax

  let test_all store =
    let gnode = Node.Group.root in
    let* nodes = hierarchy store in
    assert_equal ~printer:print_node_pair ([], []) nodes;

    let* () = Group.create store gnode in
    let* exists = Group.exists store gnode in
    assert_equal ~printer:string_of_bool true exists;
    let* nodes = hierarchy store in
    assert_equal ~printer:print_node_pair ([], [gnode]) nodes;

    let* meta = Group.metadata store gnode in
    assert_equal ~printer:Metadata.Group.show Metadata.Group.default meta;

    let* () = Group.delete store gnode in
    let* exists = Group.exists store gnode in
    assert_equal ~printer:string_of_bool false exists;
    let* nodes = hierarchy store in
    assert_equal ~printer:print_node_pair ([], []) nodes;

    let attrs = `Assoc [("questions", `String "answer")] in
    let* () = Group.create ~attrs store gnode in
    let* meta = Group.metadata store gnode in
    assert_equal ~printer:Yojson.Safe.show attrs @@ Metadata.Group.attributes meta;

    let* exists = IO.bind (IO.lift Node.Array.(gnode / "non-member")) (Array.exists store) in
    assert_equal ~printer:string_of_bool false exists;

    let shard_configs = [
      {chunk_shape = [2; 5; 5]
      ;index_location = Start
      ;index_codecs = [`Bytes BE; `Crc32c]
      ;codecs = [`Transpose [2; 0; 1]; `Bytes LE; `Zstd (0, false)]};
      {chunk_shape = [2; 5; 5]
      ;index_location = Start
      ;index_codecs = [`Bytes LE]
      ;codecs = [`Bytes BE]}
    ] in
    let slice = [R (0, 20); I 10; R (0, 29)] in
    let bigger_slice =  [R (0, 21); L [9; 10] ; R (0, 30)] in
    let anode = fold_result Node.Array.(gnode / "arrnode") in

    let* () =
      List.fold_left2
        (fun acc cfg anode ->
          let* () = acc in
          let* () = Array.create ~overwrite:true ~codecs:[`ShardingIndexed cfg] ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Complex32 Complex.one anode store in
          let exp = Ndarray.init Complex32 [21; 1; 30] (Fun.const Complex.one) in
          let* got = Array.read store anode slice Complex32 in
          assert_equal exp got;
          Ndarray.fill exp Complex.{re=2.0; im=0.};
          let* () = Array.write store anode slice exp in
          let* got = Array.read store anode slice Complex32 in
          (* test if a bigger slice containing new elements can be read from store *)
          let* _ = Array.read store anode bigger_slice Complex32 in
          assert_equal exp got;
          (* test writing a bigger slice to store *)
          let* () = Array.write store anode bigger_slice (Ndarray.init Complex32 [22; 2; 31] (Fun.const Complex.{re=0.; im=3.0})) in
          let* got = Array.read store anode slice Complex32 in
          Ndarray.fill exp Complex.{re=0.; im=3.0};
          assert_equal exp got;
          acc)
       (Ok ()) shard_configs [anode; anode]
    in
    (* test failure to create an array that already exists if overwrite flag is not set. *)
    assert_equal
      (Error (`Node_already_exists "/arrnode"))
      (Array.create ~sep:`Dot ~codecs:[`Bytes BE] ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Ndarray.Int Int.max_int anode store);

    (* repeat tests for non-sharding codec chain *)
    let* () = Array.create ~overwrite:true ~sep:`Dot ~codecs:[`Bytes BE] ~shape:[100; 100; 50] ~chunks:[10; 15; 20] Ndarray.Int Int.max_int anode store in
    let* got = hierarchy store in
    assert_equal ~printer:print_node_pair ([anode], [gnode]) got;
    (* test path where there is no chunk key present in store *)
    let exp = Ndarray.init Int [21; 1; 30] (Fun.const Int.max_int) in
    let* () = Array.write store anode slice exp in
    let* got = Array.read store anode slice Int in
    assert_equal exp got;
    (* test path where there is a chunk key present in store at write time. *)
    let* () = Array.write store anode slice exp in
    let* got = Array.read store anode slice Int in
    assert_equal exp got;

    assert_equal (Error `Invalid_data_type) (Array.read store anode slice Ndarray.Char);
    let badslice = [R (0, 20); I 10; F; F] in
    assert_equal (Error `Invalid_array_slice) (Array.read store anode badslice Ndarray.Int);
    assert_equal (Error `Invalid_array_slice) (Array.write store anode badslice exp);
    assert_equal (Error `Invalid_array_slice) (Array.write store anode [R (0, 20); F; F] exp);
    let badarray = Ndarray.init Float64 [21; 1; 30] (Fun.const 0.) in
    assert_equal (Error `Invalid_data_type) (Array.write store anode slice badarray);

    let child = fold_result (Node.Group.of_path "/some/child/group") in
    let* () = Group.create store child in
    let* arrays, groups = Group.children store gnode in
    assert_equal ~printer:string_of_list ["/arrnode"] (List.map Node.Array.to_path arrays);
    assert_equal ~printer:string_of_list ["/some"] (List.map Node.Group.to_path groups);
    let* got = Group.children store child in
    assert_equal ([], []) got;
    let* got = Group.children store (fold_result Node.Group.(root / "fakegroup")) in
    assert_equal ([], []) got;

    let* ac, gc = hierarchy store in
    let got = List.fast_sort String.compare (List.map Node.Array.show ac @ List.map Node.Group.show gc) in
    assert_equal ~printer:string_of_list ["/"; "/arrnode"; "/some"; "/some/child"; "/some/child/group"] got;

    (* tests for renaming nodes *)
    let some = fold_result (Node.Group.of_path "/some/child") in
    let* _ = Group.rename store some "CHILD" in
    let* anode' = Array.rename store anode "ARRAYNODE" in
    let* ac, gc = hierarchy store in
    let got = List.fast_sort String.compare (List.map Node.Array.show ac @ List.map Node.Group.show gc) in
    assert_equal ~printer:string_of_list ["/"; "/ARRAYNODE"; "/some"; "/some/CHILD"; "/some/CHILD/group"] got;
    (* restore old array node name. *)
    let* got = Array.rename store anode' "arrnode" in
    assert_equal anode got;
    assert_equal
      (Error (`Key_not_found "fakegroup"))
      (Result.bind Node.Group.(gnode / "fakegroup") (fun g -> Group.rename store g "somename"));
    assert_equal
      (Error (`Key_not_found "fakearray"))
      (Result.bind Node.Array.(gnode / "fakearray") (fun g -> Array.rename store g "somename"));
    let nshape = [25; 32; 10] in
    let* () = Array.reshape store anode nshape in
    let* meta = Array.metadata store anode in
    assert_equal ~printer:[%show : int list] nshape (Metadata.Array.shape meta);
    assert_equal (Error `Invalid_resize_shape) (Array.reshape store anode [25; 10]);
    match Result.bind Node.Array.(gnode / "fakegroup") (Array.metadata store) with
    | Ok _ -> assert_failure "requesting metadata of non-existant nodes should not work.";
    | Error _ -> ();
    
    let* () = Array.delete store anode in
    let* () = clear store in
    let* got = hierarchy store in
    assert_equal ~printer:print_node_pair ([], []) got;
    IO.return_unit
end

let test_zip_store = "zip archive store tests" >:: (fun _ ->
  let rand_num = string_of_int (Random.int 100) in
  let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
  let module ZipStoreTester = Make (struct type t = Zip.error end) (ZipStore) in
  (* test with non-existant archive *)
  let zpath = tmp_dir ^ ".zip" in
  (match ZipStoreTester.test_all (fold_result @@ ZipStore.create zpath) with
  | Ok () -> ()
  | Error _ -> assert_failure "test suite is not supposed to fail");
  (* test just opening the now existant archive created by the previous test. *)
  ignore (fold_result @@ ZipStore.open_store zpath);
  (* test if creating an existing zip store fails. *)
  assert_equal (Error (`Zarr (`Read (zpath ^ ": File already exists.")))) (ZipStore.create zpath);
  assert_equal
    (Error (`Zarr (`Read ("fakefile.zip: File does not exist."))))
    (ZipStore.open_store "fakefile.zip");
  let levels = [L0; L1; L2; L3; L4; L5; L7; L8; L9] in
  List.iter (fun level -> fold_result (ZipStore.open_store ~level zpath) |> ignore) levels;
)

let test_memory_store = "memory store tests" >:: (fun _ ->
  let module MemStoreTester = Make (struct type t = Memory.error end) (MemoryStore) in
  match MemStoreTester.test_all (MemoryStore.create ()) with
  | Ok () -> ()
  | Error _ -> assert_failure "test suite is not supposed to fail"
)

let test_filesystem_store = "filesystem store tests" >:: (fun _ ->
  let rand_num = string_of_int (Random.int 100) in
  let tmp_dir = Filename.(concat (get_temp_dir_name ()) (rand_num ^ ".zarr")) in
  let module FSStoreTester = Make (struct type t = FilesystemStore.error end) (FilesystemStore) in
  let s = fold_result (FilesystemStore.create tmp_dir) in
  (match FSStoreTester.test_all s with
  | Ok () -> ()
  | Error _ -> assert_failure "test suite is not supposed to fail");
  (* ensure it works with an extra "/" appended to directory name. *)
  ignore (fold_result @@ FilesystemStore.open_store (tmp_dir ^ "/"));
  (* test if opening a non existant store fails. *)
  let fakedir = "non-existant-zarr-store12345.zarr" in
  assert_equal
    (Error (`Zarr (`Read (Printf.sprintf "%s: No such file or directory" fakedir))))
    (FilesystemStore.open_store fakedir);
  let fn = Filename.temp_file "nonexistantfile" ".zarr" in
  assert_equal
    (Error (`Zarr (`Read (Printf.sprintf "%s is not a directory." fn))))
    (FilesystemStore.open_store fn);
  (* test if creating an existing fs store fails. *)
  assert_equal
    (Error (`Zarr (`Write (Format.sprintf "%s: File exists" tmp_dir))))
    (FilesystemStore.create tmp_dir);
  (* inject a bad metadata document to test correct parsing of bad child
     nodes when discovering children of a group. *)
  let dname = tmp_dir ^ "/badnode" in
  let fname = Filename.concat dname "zarr.json" in
  Sys.mkdir dname 0o700;
  Out_channel.with_open_bin fname (Fun.flip Out_channel.output_string {|{"zarr_format":3,"node_type":"unknown"}|});
  assert_equal
    (Error (`Parse_error "invalid node_type in badnode/zarr.json"))
    (FilesystemStore.hierarchy s);
  Sys.(remove fname; rmdir dname);
)

let _ =
  run_test_tt_main ("Run Zarr sync API test suite" >::: [
    test_filesystem_store;
    test_memory_store;
    test_zip_store;
  ])
