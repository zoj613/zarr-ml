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
    assert_failure "Decoding well formed metadata should not fail");
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

let array = [

"array metadata" >:: (fun _ ->
  let shape = [|10; 10; 10|] in
  let chunks = [|5; 2; 6|] in
  let grid_shape = [|2; 5; 2|] in
  let dimension_names = [Some "x"; None; Some "z"] in

  let meta =
    ArrayMetadata.create
      ~shape ~dimension_names Bigarray.Float32 32.0 chunks
  in
  (match ArrayMetadata.encode meta |> ArrayMetadata.decode with
  | Ok v ->
    assert_bool "should not fail" @@ ArrayMetadata.equal v meta;
  | Error _ ->
    assert_failure "Decoding well formed metadata should not fail");

  assert_bool
    "" (Result.is_error @@ ArrayMetadata.decode {|{"bad_json":0}|});

  let show_int_array = [%show: int array] in
  assert_equal
    ~printer:show_int_array shape @@ ArrayMetadata.shape meta;

  assert_equal
    ~printer:Codecs.Chain.show
    Codecs.Chain.default @@
    ArrayMetadata.codecs meta;

  assert_equal
    ~printer:string_of_int
    (Array.length shape)
    (ArrayMetadata.ndim meta);

  assert_equal
    ~printer:show_int_array
    chunks @@
    ArrayMetadata.chunk_shape meta;

  assert_equal
    ~printer:show_int_array
    grid_shape @@
    ArrayMetadata.grid_shape meta shape;

  let show_int_array_tuple =
    [%show: int array * int array]
  in
  assert_equal
    ~printer:show_int_array_tuple
    ([|1; 3; 1|], [|3; 1; 0|]) @@
    ArrayMetadata.index_coord_pair meta [|8; 7; 6|];

  assert_equal
    ~printer:show_int_array_tuple
    ([|2; 5; 1|], [|0; 0; 4|]) @@
    ArrayMetadata.index_coord_pair meta [|10; 10; 10|];

  assert_equal
    ~printer:Fun.id
    "c/2/5/1" @@
    ArrayMetadata.chunk_key meta [|2; 5; 1|];

  let indices =
    [[|0; 0; 0|]; [|0; 0; 1|]; [|0; 1; 0|]; [|0; 1; 1|]
    ;[|1; 0; 0|]; [|1; 0; 1|]; [|1; 1; 0|]; [|1; 1; 1|]]
  in
  assert_equal
    ~printer:[%show: int array list]
    indices @@
    ArrayMetadata.chunk_indices meta [|10; 4; 10|];

  assert_equal
    ~printer:Fun.id
    {|"float32"|} @@
    ArrayMetadata.data_type meta;

  assert_equal
    ~printer:[%show: string option list]
    dimension_names @@
    ArrayMetadata.dimension_names meta;

  assert_equal
    ~printer:Yojson.Safe.show
    `Null @@
    ArrayMetadata.attributes meta;

  let attrs = `Assoc [("questions", `String "answer")] in
  assert_equal
    ~printer:Yojson.Safe.show
    attrs
    ArrayMetadata.(attributes @@ update_attributes meta attrs);

  let new_shape = [|20; 10; 6|] in
  assert_equal
    ~printer:show_int_array
    new_shape @@
    ArrayMetadata.(shape @@ update_shape meta new_shape);

  assert_bool
    "" @@ ArrayMetadata.is_valid_kind meta Bigarray.Float32;

  assert_bool
    "Float32 is the only valid kind for this metadata"
    (not @@ ArrayMetadata.is_valid_kind meta Bigarray.Int8_signed);

  assert_equal
    ~printer:string_of_float
    32. @@
    ArrayMetadata.fillvalue_of_kind meta Bigarray.Float32;

  assert_raises
    ~msg:"Wrong kind used to extract fill value."
    (Failure "kind is not compatible with node's fill value.")
    (fun () -> ArrayMetadata.fillvalue_of_kind meta Bigarray.Complex32))
]

let tests = group @ array
