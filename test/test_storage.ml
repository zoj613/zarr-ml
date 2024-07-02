open OUnit2
open Zarr
open Zarr.Storage

module Ndarray = Owl.Dense.Ndarray.Generic

let string_of_list = [%show: string list]

let test_store
  (type a) (module M : Zarr.Storage.S with type t = a) (store : a) =
  let gnode = Node.root in

  M.create_group store gnode;
  assert_equal
    ~printer:string_of_bool
    true @@
    M.is_member store gnode;

  (match M.group_metadata gnode store with
  | Ok meta ->
    assert_equal
      ~printer:GroupMetadata.show GroupMetadata.default meta
  | Error _ ->
    assert_failure
      "group node created with default values should
      have metadata with default values.");

  M.erase_node store gnode;
  assert_bool
    "Cannot retrive metadata of a node not in the store." @@
    Result.is_error @@ M.group_metadata gnode store;
  assert_equal 
    ~printer:[%show: Node.t list]
    [] @@
    M.find_all_nodes store;

  let attrs = `Assoc [("questions", `String "answer")] in
  M.create_group 
    ~metadata:GroupMetadata.(update_attributes default attrs)
    store
    gnode;
  (match M.group_metadata gnode store with
  | Ok meta ->
    assert_equal
      ~printer:Yojson.Safe.show
      attrs @@
      GroupMetadata.attributes meta
  | Error _ ->
    assert_failure
      "group node created with specified values should
      have metadata with said values.");

  let fake = Node.(gnode / "non-member") |> Result.get_ok in
  assert_equal
    ~printer:string_of_bool
    false @@
    M.is_member store fake;

  let anode = Node.(gnode / "arrnode") |> Result.get_ok in
  let r =
    M.create_array
      ~shape:[|100; 100; 50|]
      ~chunks:[|10; 15; 20|]
      Bigarray.Complex64
      Complex.zero
      anode
      store
  in
  assert_equal (Ok ()) r;
  
  assert_bool
    "Cannot get group metadata from an array node" @@
    Result.is_error @@ M.group_metadata anode store;

  let slice = Owl_types.[|R [0; 20]; I 10; R []|] in
  let expected =
    Ndarray.create Bigarray.Complex64 [|21; 1; 50|] Complex.zero in
  let got =
    Result.get_ok @@
    M.get_array anode slice Bigarray.Complex64 store in
  assert_equal
    ~printer:Owl_pretty.dsnda_to_string
    expected
    got;

  let x' = Ndarray.map (fun _ -> Complex.one) got in
  let r = M.set_array anode slice x' store in
  assert_equal (Ok ()) r;
  let got =
    Result.get_ok @@
    M.get_array anode slice Bigarray.Complex64 store
  in
  assert_equal ~printer:Owl_pretty.dsnda_to_string x' got;
  assert_bool
    "get_array can only work with the correct array kind" @@
    Result.is_error @@ M.get_array anode slice Bigarray.Int32 store;
  assert_bool
    "get_array slice shape must be the same as the array's." @@
    Result.is_error @@
    M.get_array
      anode
      Owl_types.[|R [0; 20]; I 10; R []; R [] |]
      Bigarray.Complex64
      store;

  let bad_slice = Owl_types.[|R [0; 20]; I 10; I 0|] in
  assert_bool
    "slice written to store must have the same
    shape as the array to be written" @@
    Result.is_error @@
    M.set_array anode bad_slice x' store;
  let bad_arr =
    Ndarray.create Bigarray.Int32 [|21; 1; 50|] Int32.max_int in
  assert_bool
    "slice written to store must have the same
    shape as the array to be written" @@
    Result.is_error @@
    M.set_array anode slice bad_arr store;

  let child = Node.of_path "/some/child" |> Result.get_ok in
  M.create_group store child;
  (match M.find_child_nodes store gnode with
  | Ok (arrays, groups) ->
    assert_equal
      ~printer:string_of_list
      ["/arrnode"] @@
      List.map Node.to_path arrays;
    assert_equal
      ~printer:string_of_list
      ["/some"] @@
      List.map Node.to_path groups
  | Error _ ->
    assert_failure
      "a store with more than one node
      should return children for a root node.");

  assert_bool
    "Array nodes cannot have children"
    (Result.is_error @@ M.find_child_nodes store anode);

  let got =
    M.find_all_nodes store
    |> List.map Node.show
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
    M.array_metadata anode store in
  assert_equal
    ~printer:[%show: int array]
    new_shape @@
    ArrayMetadata.shape meta;
  assert_bool
    "Group nodes cannot be reshaped" @@
    Result.is_error @@ M.reshape store gnode new_shape;
  assert_bool
    "New shape must have the number of dims as the node." @@
    Result.is_error @@ M.reshape store anode [|25; 10|];

  assert_bool
    "Cannot get array metadata from a group node" @@
    Result.is_error @@ M.array_metadata gnode store;
  assert_bool
    "Cannot get array metadata from a node not a member of store" @@
    Result.is_error @@ M.array_metadata fake store;

  M.erase_node store anode


let tests = [
  "test in-memory store" >::
    (fun _ ->
      test_store
        (module MemoryStore) @@ MemoryStore.create ())
;
  "test filesystem store" >::
    (fun _ ->
      let tmp_dir = Filename.get_temp_dir_name () ^ ".zarr" in
      Sys.mkdir tmp_dir 0o777;
      match FilesystemStore.open_or_create ~file_perm:0o777 tmp_dir with
      | Ok s -> test_store (module FilesystemStore) s
      | Error _ ->
        assert_failure "FilesystemStore creation should not fail.")
]
