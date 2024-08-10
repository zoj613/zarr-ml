open OUnit2
open Zarr

let flatten_fstring s =
  String.(split_on_char ' ' s |> concat "" |> split_on_char '\n' |> concat "")

let decode_bad_group_metadata ~str ~msg = 
  match GroupMetadata.decode str with
  | Ok _ ->
    assert_failure
      "Impossible to decode an ill-formed JSON group metadata document.";
  | Error (`Store_read s) ->
    assert_equal ~printer:Fun.id msg s

let group = [
"group metadata" >:: (fun _ ->
  let meta = GroupMetadata.default in
  let expected = {|{"zarr_format":3,"node_type":"group"}|} in
  let got = GroupMetadata.encode meta in
  assert_equal ~printer:Fun.id expected got;

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
  assert_equal expected @@ GroupMetadata.encode meta';

  (* test bad zarr_format field value. *)
  decode_bad_group_metadata
    ~str:{|{"zarr_format":[],"node_type":"group"}|}
    ~msg:"zarr_format field must be the integer 3.";

  (* test missing node_type field or bad value. *)
  decode_bad_group_metadata
    ~str:{|{"zarr_format":3,"node_type":"ARRAY"}|}
    ~msg:"node_type field must be 'group.";
  decode_bad_group_metadata
    ~str:{|{"zarr_format":3}|}
    ~msg:"group metadata must contain a node_type field.")
]

let test_array_metadata
  : type a b c d .
    ?dimension_names:string option list ->
    shape:int array ->
    chunks:int array ->
    (a, b) Bigarray.kind ->
    (c, d) Bigarray.kind ->
    a ->
    unit
  = fun ?dimension_names ~shape ~chunks kind bad_kind fv ->
  let repr = Codecs.{kind; fill_value = fv; shape = chunks} in
  let codecs = Result.get_ok @@ Codecs.Chain.create repr [`Bytes LE] in
  let meta =
    match dimension_names with
    | Some d ->
      ArrayMetadata.create ~codecs ~shape ~dimension_names:d kind fv chunks
    | None ->
      ArrayMetadata.create ~codecs ~shape kind fv chunks
  in
  (match ArrayMetadata.encode meta |> ArrayMetadata.decode with
  | Ok v ->
    assert_bool "should not fail" @@ ArrayMetadata.(v = meta);
  | Error _ ->
    assert_failure "Decoding well formed metadata should not fail");

  assert_bool
    "" (Result.is_error @@ ArrayMetadata.decode {|{"bad_json":0}|});

  let show_int_array = [%show: int array] in
  assert_equal
    ~printer:show_int_array shape @@ ArrayMetadata.shape meta;


  assert_equal
    ~printer:show_int_array
    chunks @@
    ArrayMetadata.chunk_shape meta;

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
    ~printer:[%show: string option list]
    (if dimension_names = None then [] else Option.get dimension_names)
    (ArrayMetadata.dimension_names meta);

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
    "Using the correct kind must not fail this op" @@
    ArrayMetadata.is_valid_kind meta kind;

  assert_bool
    "Float32 is the only valid kind for this metadata"
    (not @@ ArrayMetadata.is_valid_kind meta bad_kind);

  assert_equal fv @@ ArrayMetadata.fillvalue_of_kind meta kind;

  assert_raises
    ~msg:"Wrong kind used to extract fill value."
    (Failure "kind is not compatible with node's fill value.")
    (fun () -> ArrayMetadata.fillvalue_of_kind meta bad_kind)

(* test decoding an ill-formed array metadata with an expected error message.*)
let decode_bad_array_metadata ~str ~msg = 
  match ArrayMetadata.decode str with
  | Ok _ ->
    assert_failure
      "Impossible to decode an ill-formed JSON array metadata document.";
  | Error (`Store_read s) ->
    assert_equal ~printer:Fun.id msg s

let test_encode_decode_fill_value fv =
  let str = Format.sprintf {|{
    "zarr_format": 3,
    "shape": [10000, 1000],
    "node_type": "array",
    "data_type": "float64",
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": %s,
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}},
    "chunk_key_encoding": {"name": "default"},
    "attributes": {"question": 7}}|} fv
  in
  match ArrayMetadata.decode str with
  | Ok meta ->
    assert_equal
      ~printer:Fun.id (flatten_fstring str) (ArrayMetadata.encode meta)
  | Error _ ->
    assert_failure
      "Decoding a well formed Array metadata doc should not fail."

let test_decode_encode_chunk_key name sep (key, exp_encode, exp_null) =
  let str = Format.sprintf {|{
    "zarr_format": 3,
    "shape": [10000, 1000],
    "node_type": "array",
    "data_type": "float64",
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": 0.0,
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}},
    "chunk_key_encoding":
      {"name": %s, "configuration": {"separator": %s}}}|} name sep
  in
  match ArrayMetadata.decode str with
  | Ok meta ->
    assert_equal
      ~printer:Fun.id
      exp_encode @@
      ArrayMetadata.chunk_key meta key;
    assert_equal
      ~printer:Fun.id
      exp_null @@
      ArrayMetadata.chunk_key meta [||];
    assert_equal
      ~printer:Fun.id (flatten_fstring str) @@ ArrayMetadata.encode meta
  | Error _ ->
    assert_failure
      "Decoding a well formed Array metadata should not fail."

let array = [
"array metadata" >:: (fun _ ->

  (* test missing zarr_format field and non-specific value. *)
  let str = {|{
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"array metadata must contain a zarr_format field.";
  let str = {|{
    "zarr_format": "3",
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"zarr_format field must be the integer 3.";

  (* test missing node_type field or wrong value. *)
  let str = {|{
    "zarr_format": 3,
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding": {"name": "v2"},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"array metadata must contain a node_type field.";
  let str = {|{
    "zarr_format": 3,
    "node_type": "group",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"node_type field must be 'array'.";

  (* test missing shape field, and incorrect values *)
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, -1000],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"shape field list must only contain positive integers.";
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": 5,
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"shape field must be a list of integers.";
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"array metadata must contain a shape field.";

  (* test missing codecs field or wrong codec config. *)
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding": {"name": "v2"},
    "codecs": 
      {"name": "bytes", "configuration": {"endian": "big"}},
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"codecs field must be a list of objects.";
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"array metadata must contain a codecs field.";

  (* tests incorrect dimension_name field values and incorrect size. *)
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "dimension_names": ["rows", 12345],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"dimension_names must contain strings or null values.";
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "dimension_names": ["rows", null, null],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str
    ~msg:"dimension_names length and array dimensionality must be equal.";
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "dimension_names": "rows",
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}}}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"dimension_names field must be a list.";

  (* test if storage transformer unsupported error is reported. *) 
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}},
    "storage_transformers": ["CACHE"]}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"storage_transformers field is not yet supported.";

  (* test missing chunk grid field. *)
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000"}|} in
  decode_bad_array_metadata
    ~str:str ~msg:"array metadata must contain a chunk_grid field.";

  (* test if the decoding fails if regular grid chunk shape is empty,
   * has non-positive integer values or the grid name is unsupported *)
  let template = Format.sprintf {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_grid":
      {"name": %s, "configuration": {"chunk_shape": %s}},
    "chunk_key_encoding":
      {"name": "v2", "configuration": {"separator": "."}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000"}|}
  in
  decode_bad_array_metadata
    ~str:(template {|"regular"|} {|[1, 20, 20]|}) ~msg:"grid shape mismatch.";
  decode_bad_array_metadata
    ~str:(template {|"regular"|} {|[100000, 20]|}) ~msg:"grid shape mismatch.";
  decode_bad_array_metadata
    ~str:(template {|"regular"|} {|[-4, 4]|})
    ~msg:"Regular grid chunk_shape must only contain positive integers.";
  decode_bad_array_metadata
    ~str:(template {|"UNKNOWN"|} {|[2, 4]|})
    ~msg:"Invalid Chunk grid name or configuration.";

  (* test if decoding a chunk  key encoding field without a configuration
     leads to a default value being used. *)
  let str = {|{
    "zarr_format": 3,
    "shape": [10000, 1000],
    "node_type": "array",
    "data_type": "float64",
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [10, 10]}},
    "chunk_key_encoding": {"name": "v2"}}|} in
  (match ArrayMetadata.decode str with
  | Ok meta ->
    (* we except it to use the default "." separator. *)
    assert_equal
      ~printer:Fun.id "2.0.1" @@ ArrayMetadata.chunk_key meta [|2; 0; 1|];
    (* we expect the default (unspecified) config seperator to be
       dropped when serializing the metadata to JSON format. *)
    assert_equal
      ~printer:Fun.id
      Yojson.Safe.(from_string str |> to_string) @@
      ArrayMetadata.encode meta;
  | Error _ ->
    assert_failure
      "Decoding a well formed array JSON metadata should not fail.");

  (* test if the decoding fails if chunk key encoding contains unknown
   * separator or name. *)
  let str = {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "float64",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [10, 10]}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": "0x7fc00000"}|} in
  decode_bad_array_metadata
    ~str ~msg:"array metadata must contain a chunk_key_encoding field.";

  let template = Format.sprintf {|{
    "zarr_format": 3,
    "node_type": "array",
    "shape": [10000, 1000],
    "data_type": "complex64",
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [10, 20]}},
    "chunk_key_encoding":
      {"name": %s, "configuration": {"separator": %s}},
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": ["Infinity", "0x7fc00000"]}|}
  in
  decode_bad_array_metadata
    ~str:(template {|"default"|} {|"_"|})
    ~msg:"Invalid chunk key encoding configuration.";
  decode_bad_array_metadata
    ~str:(template {|"V3"|} {|"."|})
    ~msg:"Invalid chunk key encoding configuration.";

  (* test if the decoding fails if data type is missing not
     a string or unsupported *)
  decode_bad_array_metadata
    ~str:{|{
      "zarr_format": 3,
      "node_type": "array",
      "shape": [10000, 1000],
      "data_type": 0,
      "chunk_grid":
        {"name": "regular", "configuration": {"chunk_shape": [10, 20]}},
      "chunk_key_encoding":
        {"name": "v2", "configuration": {"separator": "/"}},
      "codecs": [
        {"name": "bytes", "configuration": {"endian": "big"}}],
      "fill_value": "NaN"}|}
    ~msg:"data_type field must be a string.";
  decode_bad_array_metadata
    ~str:{|{
      "zarr_format": 3,
      "node_type": "array",
      "shape": [10000, 1000],
      "chunk_grid":
        {"name": "regular", "configuration": {"chunk_shape": [10, 20]}},
      "chunk_key_encoding":
        {"name": "v2", "configuration": {"separator": "/"}},
      "codecs": [
        {"name": "bytes", "configuration": {"endian": "big"}}],
      "fill_value": "NaN"}|}
    ~msg:"array metadata must contain a data_type field.";
  decode_bad_array_metadata
    ~str:{|{
      "zarr_format": 3,
      "node_type": "array",
      "shape": [10000, 1000],
      "data_type": "INFINITE_PRECISION",
      "chunk_grid":
        {"name": "regular", "configuration": {"chunk_shape": [10, 20]}},
      "chunk_key_encoding":
        {"name": "v2", "configuration": {"separator": "/"}},
      "codecs": [
        {"name": "bytes", "configuration": {"endian": "big"}}],
      "fill_value": "NaN"}|}
    ~msg:"Unsupported metadata data_type";

  (* test missing fill_value field. *)
  decode_bad_array_metadata
    ~str:{|{
      "zarr_format": 3,
      "node_type": "array",
      "shape": [10000, 1000],
      "data_type": "nativeint",
      "chunk_grid":
        {"name": "regular", "configuration": {"chunk_shape": [10, 20]}},
      "chunk_key_encoding":
        {"name": "v2", "configuration": {"separator": "/"}},
      "codecs": [
        {"name": "bytes", "configuration": {"endian": "big"}}]}|}
    ~msg:"array metadata must contain a fill_value field.";
  (* test if the JSON document fill value form is preserved when decoding
   * and encoding back into a JSON.
   * See: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#fill-value *)
  test_encode_decode_fill_value {|false|};
  test_encode_decode_fill_value {|0|};
  test_encode_decode_fill_value {|5.9|};
  test_encode_decode_fill_value {|"Infinity"|};
  test_encode_decode_fill_value {|"-Infinity"|};
  test_encode_decode_fill_value {|"NaN"|};
  test_encode_decode_fill_value {|"?"|};
  test_encode_decode_fill_value {|"0x7fc00000"|};
  test_encode_decode_fill_value {|[10, 0]|};
  test_encode_decode_fill_value {|[10.0, 0.0]|};
  test_encode_decode_fill_value {|["0x7fc00000", "0x7fc00000"]|};
  test_encode_decode_fill_value {|["0x7fc00000", "NaN"]|};
  test_encode_decode_fill_value {|["Infinity", "0x7fc00000"]|};
  test_encode_decode_fill_value {|["NaN", "Infinity"]|};
  (* tests decoding failure of unsupported fill value. *)
  let template = Format.sprintf {|{
    "zarr_format": 3,
    "shape": [10000, 1000],
    "node_type": "array",
    "data_type": "float64",
    "codecs": [
      {"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": %s,
    "chunk_grid":
      {"name": "regular", "configuration": {"chunk_shape": [100, 10]}},
    "chunk_key_encoding":
      {"name": "default", "configuration": {"separator": "."}}}|}
    in
  (* we dont support float literals as strings *)
  decode_bad_array_metadata
    ~str:(template {|["0.5", "5.0"]|})
    ~msg:"Unsupported fill value.";
  decode_bad_array_metadata
    ~str:(template {|["Infinity", "?"]|})
    ~msg:"Unsupported fill value.";
  (* a complex number cannot be a list with less or more than 2 elements. *)
  decode_bad_array_metadata
    ~str:(template {|[1, 4, 3]|})
    ~msg:"Unsupported fill value.";

  (* Test correctness of chunk-key encoding of keys. *)
  test_decode_encode_chunk_key
    {|"default"|} {|"/"|} ([|5; 32; 4|], "c/5/32/4", "c");
  test_decode_encode_chunk_key
    {|"default"|} {|"."|} ([|5; 32; 4|], "c.5.32.4", "c");
  test_decode_encode_chunk_key
    {|"v2"|} {|"/"|} ([|5; 32; 4|], "5/32/4", "0");
  test_decode_encode_chunk_key
    {|"v2"|} {|"."|} ([|5; 32; 4|], "5.32.4", "0");

  let shape = [|10; 10; 10|] in
  let chunks = [|5; 2; 6|] in
  let dimension_names = [Some "x"; None; Some "z"] in

  (* tests using char data type. *)
  test_array_metadata
    ~shape
    ~chunks
    Bigarray.Char
    Bigarray.Float32
    '?';

  (* tests using int8 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Int8_signed
    Bigarray.Float32
    0;

  (* tests using uint8 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Int8_unsigned
    Bigarray.Float32
    0;

  (* tests using int16 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Int16_signed
    Bigarray.Float32
    0;

  (* tests using uint16 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Int16_unsigned
    Bigarray.Float32
    0;

  (* tests using int32 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Int32
    Bigarray.Float32
    Int32.max_int;

  (* tests using int64 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Int64
    Bigarray.Float32
    0L;

  (* tests using float32 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Float32
    Bigarray.Int
    Float.neg_infinity;

  (* tests using float64 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Float64
    Bigarray.Int
    Float.neg_infinity;

  (* tests using complex32 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Complex32
    Bigarray.Float32
    Complex.zero;

  (* tests using complex64 data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Complex64
    Bigarray.Float32
    Complex.zero;

  (* tests using int data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Int
    Bigarray.Float32
    Int.max_int;

  (* tests using nativeint data type. *)
  test_array_metadata
    ~dimension_names
    ~shape
    ~chunks
    Bigarray.Nativeint
    Bigarray.Float32
    0n)
]

let tests = group @ array
