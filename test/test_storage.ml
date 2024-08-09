open OUnit2
open Zarr
open Zarr.Node
open Zarr.Storage
open Zarr.Codecs

module Ndarray = Owl.Dense.Ndarray.Generic

let string_of_list = [%show: string list]

let test_store
  (type a) (module M : Zarr.Storage.S with type t = a) (store : a) =
  let gnode = GroupNode.root in

  M.create_group store gnode;
  assert_equal
    ~printer:string_of_bool
    true @@
    M.group_exists store gnode;

  (match M.group_metadata store gnode with
  | Ok meta ->
    assert_equal
      ~printer:GroupMetadata.show GroupMetadata.default meta
  | Error _ ->
    assert_failure
      "group node created with default values should
      have metadata with default values.");

  M.erase_group_node store gnode;
  (* tests deleting a non-existant group *)
  M.erase_group_node store GroupNode.(root / "nonexist");

  assert_bool
    "Cannot retrive metadata of a node not in the store." @@
    Result.is_error @@ M.group_metadata store gnode;
  assert_equal 
    ~printer:[%show: ArrayNode.t list * GroupNode.t list]
    ([], []) @@
    M.find_all_nodes store;

  let attrs = `Assoc [("questions", `String "answer")] in
  M.create_group ~attrs store gnode;
  (match M.group_metadata store gnode with
  | Ok meta ->
    assert_equal
      ~printer:Yojson.Safe.show attrs @@ GroupMetadata.attributes meta
  | Error _ ->
    assert_failure
      "group node created with specified attributes should
      have metadata with said attributes.");

  let fake = ArrayNode.(gnode / "non-member") in
  assert_equal ~printer:string_of_bool false @@ M.array_exists store fake;

  let nested_sharding =
  `ShardingIndexed {
    chunk_shape = [|5; 1; 5|];
    index_location = Start;
    index_codecs = [`Bytes BE];
    codecs = [`Bytes LE; `Gzip L2]}
  in
  let cfg =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = End
    ;index_codecs = [`Bytes LE; `Crc32c]
    ;codecs = [`Transpose [|2; 0; 1|]; nested_sharding; `Gzip L1]} in
  let anode = ArrayNode.(gnode / "arrnode") in
  let r =
    M.create_array
      ~sep:`Dot
      ~codecs:[`ShardingIndexed cfg]
      ~shape:[|100; 100; 50|]
      ~chunks:[|10; 15; 20|]
      Bigarray.Complex64
      Complex.zero
      anode
      store
  in
  assert_equal (Ok ()) r;
  let slice = Owl_types.[|R [0; 20]; I 10; R []|] in
  let r = M.get_array store anode slice Bigarray.Complex64 in
  assert_bool "" @@ Result.is_ok r;
  let expected = Ndarray.create Bigarray.Complex64 [|21; 1; 50|] Complex.one in
  let r = M.set_array store anode slice expected in
  assert_equal (Ok ()) r;
  let got = Result.get_ok @@ M.get_array store anode slice Bigarray.Complex64 in
  assert_equal ~printer:Owl_pretty.dsnda_to_string expected got;
  let x' = Ndarray.map (fun v -> Complex.(add v one)) got in
  let r = M.set_array store anode slice x' in
  assert_equal (Ok ()) r;
  let r = M.get_array store anode slice Bigarray.Complex64 in
  assert_bool "" @@ Result.is_ok r;

  let r =
    M.create_array
      ~codecs:[`Bytes BE]
      ~shape:[|100; 100; 50|]
      ~chunks:[|10; 15; 20|]
      Bigarray.Complex64
      Complex.zero
      anode
      store in
  assert_equal (Ok ()) r;
  let expected = Ndarray.create Bigarray.Complex64 [|21; 1; 50|] Complex.zero in
  let got = Result.get_ok @@ M.get_array store anode slice Bigarray.Complex64 in
  assert_equal ~printer:Owl_pretty.dsnda_to_string expected got;
  let x' = Ndarray.map (fun v -> Complex.(add v one)) got in
  let r = M.set_array store anode slice x' in
  assert_equal (Ok ()) r;
  let got = Result.get_ok @@ M.get_array store anode slice Bigarray.Complex64 in
  assert_equal ~printer:Owl_pretty.dsnda_to_string x' got;
  assert_bool
    "get_array can only work with the correct array kind" @@
    Result.is_error @@ M.get_array store anode slice Bigarray.Int32;
  assert_bool
    "get_array slice shape must be the same as the array's." @@
    Result.is_error @@
    M.get_array
      store
      anode
      Owl_types.[|R [0; 20]; I 10; R []; R [] |]
      Bigarray.Complex64;

  let bad_slice = Owl_types.[|R [0; 20]; I 10; I 0|] in
  assert_bool
    "slice written to store must have the same
    shape as the array to be written" @@
    Result.is_error @@
    M.set_array store anode bad_slice x';
  let bad_arr =
    Ndarray.create Bigarray.Int32 [|21; 1; 50|] Int32.max_int in
  assert_bool
    "slice written to store must have the same
    shape as the array to be written" @@
    Result.is_error @@
    M.set_array store anode slice bad_arr;

  let child = GroupNode.of_path "/some/child" in
  M.create_group store child;
  (match M.find_child_nodes store gnode with
  | arrays, groups ->
    assert_equal
      ~printer:string_of_list
      ["/arrnode"] @@
      List.map ArrayNode.to_path arrays;
    assert_equal
      ~printer:string_of_list
      ["/some"] @@
      List.map GroupNode.to_path groups);

  (* test getting child nodes of a group not a member of this store. *)
  let g = GroupNode.(root / "fakegroup") in
  assert_equal ([], []) @@ M.find_child_nodes store g;

  let ac, gc = M.find_all_nodes store in
  let got =
    List.map ArrayNode.show ac @ List.map GroupNode.show gc
    |> List.fast_sort String.compare in
  assert_equal
    ~printer:string_of_list
    ["/"; "/arrnode"; "/some"; "/some/child"]
    got;

  let new_shape = [|25; 32; 10|] in
  let r = M.reshape store anode new_shape in
  assert_equal (Ok ()) r;
  let meta =
    Result.get_ok @@
    M.array_metadata store anode in
  assert_equal
    ~printer:[%show: int array]
    new_shape @@
    ArrayMetadata.shape meta;
  assert_bool
    "New shape must have the number of dims as the node." @@
    Result.is_error @@ M.reshape store anode [|25; 10|];

  assert_bool
    "Cannot get array metadata from a node not a member of store" @@
    Result.is_error @@ M.array_metadata store fake;

  M.erase_array_node store anode;
  (* test clearing of store *)
  M.erase_all_nodes store;
  assert_equal
    ~printer:[%show: ArrayNode.t list * GroupNode.t list]
    ([], []) @@
    M.find_all_nodes store

let tests = [

"test in-memory store" >::
  (fun _ ->
    let s = MemoryStore.create () in
    (* test if store is empty upon creation *)
    assert_equal
      ~printer:[%show: ArrayNode.t list * GroupNode.t list]
      ([], [])
      (MemoryStore.find_all_nodes s);
    test_store (module MemoryStore) s)
;

"test filesystem store" >::
  (fun _ ->
    let tmp_dir = Filename.get_temp_dir_name () ^ ".zarr/" in
    (match FilesystemStore.open_or_create tmp_dir with
    | Ok s ->
        (* test if store is empty upon creation *)
        assert_equal
          ~printer:[%show: ArrayNode.t list * GroupNode.t list]
          ([], [])
          (FilesystemStore.find_all_nodes s);
        test_store (module FilesystemStore) s
    | Error _ ->
      assert_failure "FilesystemStore creation should not fail.");

    let r = FilesystemStore.open_or_create tmp_dir in
    assert_bool
      "An existing store should not fail to open."
      (Result.is_ok r);

    (* test storage creation using named directory that already exists *)
    let err_msg =
      Format.sprintf "%s: File exists" tmp_dir in
    assert_raises
      (Sys_error err_msg)
      (fun () -> FilesystemStore.create tmp_dir);
    (* tests storage creation using a new directory *)
    let new_dir = Filename.get_temp_dir_name () ^ "newdir12345.zarr" in
    assert_bool
      "Creation of new non-existing store should not fail."
      (try
        ignore @@ FilesystemStore.create new_dir;
        true
        with
        | Sys_error _ -> false);

    (* test successful opening of an existing store. *)
    assert_bool
      "An existing store should not fail to open."
      (Result.is_ok @@ FilesystemStore.open_store new_dir);

    (* test failure of opening an non-existant store. *)
    assert_bool
      "reading a non-existant store should always fail." @@
      Result.is_error @@
      FilesystemStore.open_store "non-existant-zarr-store112345.zarr";

    (* test failure of opening a store using a file instead of directory *)
    let fn = Filename.temp_file "nonexistantfile" ".zarr" in
    assert_bool
      "reading a store from a file should always fail." @@
      Result.is_error @@ FilesystemStore.open_store fn)
]
