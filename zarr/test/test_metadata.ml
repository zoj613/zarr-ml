open OUnit2
open Zarr

let flatten_fstring s = String.(split_on_char ' ' s |> concat "" |> split_on_char '\n' |> concat "")
let decode_bad_group_metadata ~str ~msg = assert_raises (Metadata.Parse_error msg) (fun () -> Metadata.Group.decode str)

let group = [
"group metadata" >:: (fun _ ->
  let meta = Metadata.Group.default in
  let got = Metadata.Group.encode meta in
  assert_bool "should not fail" Metadata.Group.((encode meta |> decode) = meta);
  assert_equal ~printer:Fun.id {|{"zarr_format":3,"node_type":"group"}|} got;
  assert_equal ~printer:Metadata.Group.show meta Metadata.Group.(decode got);
  assert_raises
    (Metadata.Parse_error "metadata must contain a zarr_format field.")
    (fun () -> Metadata.Group.decode {|{"bad_json":0}|});
  let meta' = Metadata.Group.update_attributes meta (`Assoc [("spam", `String "ham"); ("eggs", `Int 42)]) in
  let expected = {|{"zarr_format":3,"node_type":"group","attributes":{"spam":"ham","eggs":42}}|} in
  assert_equal expected (Metadata.Group.encode meta');
  (* test bad zarr_format field value. *)
  decode_bad_group_metadata ~str:{|{"zarr_format":[],"node_type":"group"}|} ~msg:"zarr_format field must be the integer 3.";
  (* test missing node_type field or bad value. *)
  decode_bad_group_metadata ~str:{|{"zarr_format":3,"node_type":"ARRAY"}|} ~msg:"node_type field must be 'group'.";
  decode_bad_group_metadata ~str:{|{"zarr_format":3}|} ~msg:"group metadata must contain a node_type field.")
]

let test_array_metadata :
  type a b.
  ?dimension_names:string option list ->
  shape:int list ->
  chunks:int list ->
  a Ndarray.dtype ->
  b Ndarray.dtype ->
  a ->
  unit
  = fun ?dimension_names ~shape ~chunks kind bad_kind fv ->
  let codecs = Codecs.Chain.create chunks [`Bytes LE] in
  let meta = match dimension_names with
    | Some d -> Metadata.Array.create ~codecs ~shape ~dimension_names:d kind fv chunks
    | None -> Metadata.Array.create ~codecs ~shape kind fv chunks
  in
  assert_bool "should not fail" Metadata.Array.((encode meta |> decode) = meta);
  let meta' = Metadata.Array.update_shape meta (10 :: shape) in
  assert_equal ~msg:"should not be equal" false Metadata.Array.(meta' = meta);
  let show_int_list = [%show: int list] in
  assert_equal ~printer:show_int_list shape (Metadata.Array.shape meta);
  assert_equal ~printer:show_int_list chunks (Metadata.Array.chunk_shape meta);
  let show_int_list_tuple = [%show: int list * int list] in
  assert_equal ~printer:show_int_list_tuple ([1; 3; 1], [3; 1; 0]) (Metadata.Array.index_coord_pair meta [8; 7; 6]);
  assert_equal ~printer:show_int_list_tuple ([2; 5; 1], [0; 0; 4]) (Metadata.Array.index_coord_pair meta [10; 10; 10]);
  assert_equal ~printer:Fun.id "c/2/5/1" (Metadata.Array.chunk_key meta [2; 5; 1]);
  let indices = [[0; 0; 0]; [0; 0; 1]; [0; 1; 0]; [0; 1; 1] ;[1; 0; 0]; [1; 0; 1]; [1; 1; 0]; [1; 1; 1]] in
  assert_equal ~printer:[%show: int list list] indices (Metadata.Array.chunk_indices meta [10; 4; 10]);
  assert_equal
    ~printer:[%show: string option list]
    (if dimension_names = None then [] else Option.get dimension_names)
    (Metadata.Array.dimension_names meta);
  assert_equal ~printer:Yojson.Safe.show `Null (Metadata.Array.attributes meta);
  let attrs = `Assoc [("questions", `String "answer")] in
  assert_equal ~printer:Yojson.Safe.show attrs Metadata.Array.(attributes @@ update_attributes meta attrs);
  let new_shape = [20; 10; 6] in
  assert_equal ~printer:show_int_list new_shape Metadata.Array.(shape @@ update_shape meta new_shape);
  assert_bool "Using the correct kind must not fail this op" Metadata.Array.(is_valid_kind meta kind);
  assert_bool "Float32 is the only valid kind for this metadata" (not @@ Metadata.Array.is_valid_kind meta bad_kind);
  assert_equal fv Metadata.Array.(fillvalue_of_kind meta kind);
  assert_raises (Failure "kind is not compatible with node's fill value.") (fun () -> Metadata.Array.fillvalue_of_kind meta bad_kind);
  assert_raises (Metadata.Parse_error "metadata must contain a zarr_format field.") (fun () -> Metadata.Array.decode {|{"bad_json":0}|})

let test_scalar_array_metadata () =
  let codecs = Codecs.Chain.create [] [`Bytes LE] in
  let meta = Metadata.Array.create ~codecs ~shape:[] Float32 0.0 [] in
  assert_bool "should not fail" Metadata.Array.((encode meta |> decode) = meta);
  let show_int_list = [%show: int list] in
  assert_equal ~printer:show_int_list [] (Metadata.Array.shape meta);
  assert_equal ~printer:show_int_list [] (Metadata.Array.chunk_shape meta);
  let show_int_list_tuple = [%show: int list * int list] in
  assert_equal ~printer:show_int_list_tuple ([], []) (Metadata.Array.index_coord_pair meta []);
  assert_equal ~printer:[%show: int list list] [[]] (Metadata.Array.chunk_indices meta [])
  (*assert_raises
    (Metadata.Parse_error "dimension_names length and array dimensionality must be equal.")
    (fun () -> Metadata.Array.create ~codecs ~dimension_names:[Some ""] ~shape:[] Float32 0.0 []) *)

(* test decoding an ill-formed array metadata with an expected error message.*)
let decode_bad_array_metadata ~str ~msg = assert_raises (Metadata.Parse_error msg) (fun () -> Metadata.Array.decode str)

let test_encode_decode_fill_value d f1 f2 f3 =
  let fmt = Format.sprintf {|{
    "zarr_format": 3,
    "shape": [10000, 1000],
    "node_type": "array",
    "data_type": "%s",
    "codecs": [{"name": "bytes", "configuration": {"endian": "big"}}],
    "fill_value": %s,
    "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [100, 10]}},
    "chunk_key_encoding": {"name": "default"},
    "attributes": {"question": 7},
    "dimension_names": ["x", null]}|}
  in
  let str = fmt d f1 in
  let meta = Metadata.Array.decode str in
  let meta' = Metadata.Array.decode (fmt d f2) in
  assert_equal false Metadata.Array.(meta = meta');
  assert_bool "Metadata must be equal to itself." Metadata.Array.(meta = meta);
  assert_equal ~printer:Fun.id (flatten_fstring str) Metadata.Array.(encode meta);
  assert_raises (Metadata.Parse_error "Unsupported fill value.") (fun () -> Metadata.Array.decode (fmt d f3))

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
      {"name": %s, "configuration": {"separator": %s}},
    "attributes": {"question": 7}}|} name sep
  in
  let meta = Metadata.Array.decode str in
  assert_equal ~printer:Fun.id exp_encode (Metadata.Array.chunk_key meta key);
  assert_equal ~printer:Fun.id exp_null (Metadata.Array.chunk_key meta []);
  assert_equal ~printer:Fun.id (flatten_fstring str) (Metadata.Array.encode meta)

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
  decode_bad_array_metadata ~str:str ~msg:"metadata must contain a zarr_format field.";
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
  decode_bad_array_metadata ~str:str ~msg:"zarr_format field must be the integer 3.";
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
  decode_bad_array_metadata ~str:str ~msg:"metadata must contain a node_type field.";
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
  decode_bad_array_metadata ~str:str ~msg:"node_type field must be 'array'.";
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
  decode_bad_array_metadata ~str:str ~msg:"shape field list must only contain positive integers.";
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
  decode_bad_array_metadata ~str:str ~msg:"shape field must be a list of integers.";
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
  decode_bad_array_metadata ~str:str ~msg:"array metadata must contain a shape field.";
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
  decode_bad_array_metadata ~str:str ~msg:"codecs field must be a list of objects.";
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
  decode_bad_array_metadata ~str:str ~msg:"array metadata must contain a codecs field.";
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
  decode_bad_array_metadata ~str:str ~msg:"dimension_names must contain strings or null values.";
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
  decode_bad_array_metadata ~str:str ~msg:"dimension_names length and array dimensionality must be equal.";
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
  decode_bad_array_metadata ~str:str ~msg:"dimension_names field must be a list.";
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
  decode_bad_array_metadata ~str:str ~msg:"storage_transformers field is not yet supported.";
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
  decode_bad_array_metadata ~str:str ~msg:"array metadata must contain a chunk_grid field.";
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
  decode_bad_array_metadata ~str:(template {|"regular"|} {|[1, 20, 20]|}) ~msg:"grid shape mismatch.";
  decode_bad_array_metadata ~str:(template {|"regular"|} {|[100000, 20]|}) ~msg:"grid shape mismatch.";
  decode_bad_array_metadata ~str:(template {|"regular"|} {|[-4, 4]|}) ~msg:"chunk_shape must only contain positive ints.";
  decode_bad_array_metadata ~str:(template {|"UNKNOWN"|} {|[2, 4]|}) ~msg:"Invalid Chunk grid name or configuration.";
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
  let meta = Metadata.Array.decode str in
  (* we except it to use the default "." separator. *)
  assert_equal ~printer:Fun.id "2.0.1" Metadata.Array.(chunk_key meta [2; 0; 1]);
  (* we expect the default (unspecified) config seperator to be dropped when serializing the metadata to JSON format. *)
  assert_equal ~printer:Fun.id Yojson.Safe.(from_string str |> to_string) Metadata.Array.(encode meta);
  (* test if the decoding fails if chunk key encoding contains unknown separator or name.*)
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
  decode_bad_array_metadata ~str ~msg:"array metadata must contain a chunk_key_encoding field.";
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
  decode_bad_array_metadata ~str:(template {|"default"|} {|"_"|}) ~msg:"Invalid chunk key encoding configuration.";
  decode_bad_array_metadata ~str:(template {|"V3"|} {|"."|}) ~msg:"Invalid chunk key encoding configuration.";
  (* test if the decoding fails if data type is missing not a string or unsupported *)
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
    ~msg:"Unsupported metadata data_type";
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
  test_encode_decode_fill_value "char" {|"?"|} {|"-"|} {|"??"|};
  test_encode_decode_fill_value "bool" {|false|} {|true|} {|"??"|};
  test_encode_decode_fill_value "bool" {|true|} {|false|} {|"??"|};
  test_encode_decode_fill_value "int8" {|-10|} {|10|} {|5000|};
  test_encode_decode_fill_value "uint8" {|0|} {|255|} {|-1|};
  test_encode_decode_fill_value "int16" {|-1000|} {|10|} {|50000|};
  test_encode_decode_fill_value "uint16" {|0|} {|1|} {|-10.5|};
  test_encode_decode_fill_value "int32" {|0|} {|1|} {|21474836475|};
  test_encode_decode_fill_value "int" {|0|} {|1|} {|-4611686018427387909|};
  test_encode_decode_fill_value "int64" {|-4611686018427387909|} {|0|} {|18446744073709551619|};
  test_encode_decode_fill_value "uint64" {|4611686018427387909|} {|0|} {|18446744073709551619|};
  test_encode_decode_fill_value "float32" {|1|} {|0.0|} {|"adlalkjdald"|};
  test_encode_decode_fill_value "float32" {|-4611686018427387908|} {|-4611686018427387909|} {|"adlalkjdald"|};
  test_encode_decode_fill_value "float32" {|"Infinity"|} {|-4611686018427387909|} {|"adlalkjdald"|};
  test_encode_decode_fill_value "float32" {|"NaN"|} {|"-Infinity"|} {|"0x2a032f00000000000000000000000000"|};
  test_encode_decode_fill_value "float32" {|"0x7fc00000"|} {|"-Infinity"|} {|"adlalkjdald"|};
  test_encode_decode_fill_value "float64" {|0.0|} {|1|} {|"adlalkjdald"|};
  test_encode_decode_fill_value "float64" {|"Infinity"|} {|-4611686018427387909|} {|"adlalkjdald"|};
  test_encode_decode_fill_value "float64" {|"-Infinity"|} {|"NaN"|} {|"0x2a032f00000000000000000000000000"|};
  test_encode_decode_fill_value "float64" {|"-Infinity"|} {|"0x7fc00000"|} {|"adlalkjdald"|};
  test_encode_decode_fill_value "complex32" {|[1, 2]|} {|[-4611686018427387909, -4611686018427387909]|} {|[1, 0.5]|};
  test_encode_decode_fill_value "complex32" {|[1.0, 2.0]|} {|["Infinity", "NaN"]|} {|[1, 0.5]|};
  test_encode_decode_fill_value "complex64" {|[-4611686018427387909, -4611686018427387909]|} {|[1, 2]|} {|[1, 0.5]|};
  test_encode_decode_fill_value "complex64" {|["Infinity", "NaN"]|} {|[1.0, 2.0]|} {|[1, 0.5]|};
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
  decode_bad_array_metadata ~str:(template {|["0.5", "5.0"]|}) ~msg:"Unsupported fill value.";
  decode_bad_array_metadata ~str:(template {|["Infinity", "?"]|}) ~msg:"Unsupported fill value.";
  (* a complex number cannot be a list with less or more than 2 elements. *)
  decode_bad_array_metadata ~str:(template {|[1, 4, 3]|}) ~msg:"Unsupported fill value.";
  (* Test correctness of chunk-key encoding of keys. *)
  test_decode_encode_chunk_key {|"default"|} {|"/"|} ([5; 32; 4], "c/5/32/4", "c");
  test_decode_encode_chunk_key {|"default"|} {|"."|} ([5; 32; 4], "c.5.32.4", "c");
  test_decode_encode_chunk_key {|"v2"|} {|"/"|} ([5; 32; 4], "5/32/4", "0");
  test_decode_encode_chunk_key {|"v2"|} {|"."|} ([5; 32; 4], "5.32.4", "0");
  let shape = [10; 10; 10] in
  let chunks = [5; 2; 6] in
  let dimension_names = [Some "x"; None; Some "z"] in
  (* tests using bool data type. *)
  test_array_metadata ~shape ~chunks Ndarray.Bool Ndarray.Float32 false;
  (* tests using char data type. *)
  test_array_metadata ~shape ~chunks Ndarray.Char Ndarray.Float32 '?';
  (* tests using int8 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Int8 Ndarray.Float32 0;
  (* tests using uint8 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Uint8 Ndarray.Float32 0;
  (* tests using int16 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Int16 Ndarray.Float32 0;
  (* tests using uint16 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Uint16 Ndarray.Float32 0;
  (* tests using int32 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Int32 Ndarray.Float32 Int32.max_int;
  (* tests using int64 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Int64 Ndarray.Float32 0L;
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Int64 Ndarray.Float32 Int64.max_int;
  (* tests using uint64 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Uint64 Ndarray.Float32 Stdint.Uint64.min_int;
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Uint64 Ndarray.Float32 Stdint.Uint64.max_int;
  (* tests using float32 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Float32 Ndarray.Int Float.neg_infinity;
  (* tests using float64 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Float64 Ndarray.Int Float.neg_infinity;
  (* tests using complex32 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Complex32 Ndarray.Float32 Complex.zero;
  (* tests using complex64 data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Complex64 Ndarray.Float32 Complex.zero;
  (* tests using int data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Int Ndarray.Float32 Int.max_int;
  (* tests using nativeint data type. *)
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Nativeint Ndarray.Float32 0n;
  test_array_metadata ~dimension_names ~shape ~chunks Ndarray.Nativeint Ndarray.Float32 Nativeint.max_int;
  (* tests correctness when using an array with empty shape (scalar). *)
  test_scalar_array_metadata ());
]

let tests = group @ array
