open OUnit2
open Zarr

let group = [

"group metadata" >:: (fun _ ->
  let meta = GroupMetadata.default in
  let expected = {|{"zarr_format":3,"node_type":"group"}|} in
  let got = GroupMetadata.encode meta in
  assert_equal expected got;

  (match GroupMetadata.decode got with
  | Ok v ->
    assert_equal ~printer:GroupMetadata.show meta v;
  | Error _ ->
    assert_bool "Decoding well formed metadata should not fail" false);
  assert_bool "" (Result.is_error @@ GroupMetadata.decode {|{"bad_json":0}|});

  let meta' =
    GroupMetadata.update_attributes
      meta @@ `Assoc [("spam", `String "ham"); ("eggs", `Int 42)]
  in
  let expected =
    {|{"zarr_format":3,"node_type":"group","attributes":{"spam":"ham","eggs":42}}|}
  in
  assert_equal expected @@ GroupMetadata.encode meta')
]
