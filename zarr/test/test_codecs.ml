open OUnit2
open Zarr
open Zarr.Codecs

let decode_chain ~shape ~str ~msg = begin match Chain.of_yojson shape @@ Yojson.Safe.from_string str with
  | Ok _ -> assert_failure "Impossible to decode an unsupported codec.";
  | Error s -> assert_equal ~printer:Fun.id msg s end

let bytes_encode_decode (type a) (decoded_repr : a array_repr) (fill_value : a) =
    List.iter
      (fun bytes_codec ->
        let chain = [bytes_codec] in
        let c = Chain.create decoded_repr.shape chain in
        let arr = Ndarray.create decoded_repr.kind decoded_repr.shape fill_value in
        let decoded = Chain.decode c decoded_repr (Chain.encode c arr) in
        assert_equal arr decoded)
      [`Bytes LE; `Bytes BE]

let tests = [
"test codec chain" >:: (fun _ ->
  let shape = [10; 15; 10] in
  let kind = Ndarray.Int16 in
  let fill_value = 10 in
  let shard_cfg =
    {chunk_shape = [2; 5; 5]
    ;index_location = End
    ;index_codecs = [`Bytes LE; `Crc32c]
    ;codecs = [`Transpose [0; 1; 2]; `Bytes BE; `Gzip L1]}
  in
  let chain  = [`Transpose [2; 1; 0; 3]; `ShardingIndexed shard_cfg; `Crc32c; `Gzip L9] in
  assert_raises (Zarr.Codecs.Invalid_transpose_order) (fun () -> Chain.create shape chain);
  let chain = [`ShardingIndexed shard_cfg; `Transpose [2; 1; 0]; `Gzip L0] in
  assert_raises (Zarr.Codecs.Invalid_codec_ordering) (fun () -> Chain.create shape chain);
  let chain = [`Transpose [2; 1; 0]; `Crc32c] in
  assert_raises (Zarr.Codecs.Array_to_bytes_invariant) (fun () -> Chain.create shape chain);
  let chain = [`Transpose [2; 1; 0]; `ShardingIndexed shard_cfg; `Crc32c; `Gzip L9] in
  let c = Chain.create shape chain in
  let arr = Ndarray.create kind shape fill_value in
  let encoded = Chain.encode c arr in
  assert_equal arr @@ Chain.decode c {shape; kind} encoded;
  decode_chain ~shape ~str:"[]" ~msg:"Must be exactly one array->bytes codec.";
  decode_chain
    ~shape
    ~str:{|[{"name": "gzip", "configuration": {"level": 1}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  decode_chain
    ~shape
    ~str:{|[{"name": "fake_codec"}, {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"fake_codec codec is unsupported or has invalid configuration.";

  let str = Chain.to_yojson c |> Yojson.Safe.to_string in
  (match Chain.of_yojson shape @@ Yojson.Safe.from_string str with
  | Ok v -> assert_equal v c;
  | Error _ -> assert_failure "a serialized chain should successfully deserialize"))
;

"test transpose codec" >:: (fun _ ->
  (* test decoding of chain with misspelled configuration name *)
  decode_chain
    ~shape:[1; 1]
    ~str:{|[{"name": "transpose", "configuration": {"ordeR": [0, 1]}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding of chain with empty transpose order *)
  decode_chain
    ~shape:[]
    ~str:{|[{"name": "transpose", "configuration": {"order": []}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding of chain with duplicated transpose order *)
  decode_chain
    ~shape:[1; 1]
    ~str:{|[{"name": "transpose", "configuration": {"order": [0, 0]}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding with negative transpose dimensions. *)
  decode_chain
    ~shape:[1]
    ~str:{|[{"name": "transpose", "configuration": {"order": [-1]}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding transpose order bigger than an array's dimensionality. *)
    decode_chain 
    ~shape:[2; 2]
    ~str:{|[{"name": "transpose", "configuration": {"order": [0, 1, 2]}},
          {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding transpose order containing non-integer value(s). *)
    decode_chain 
    ~shape:[2; 2]
    ~str:{|[{"name": "transpose", "configuration": {"order": [0, 1, 2.0]}},
          {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test encoding of chain with an empty or too big transpose order. *)
  let shape = [2; 2; 2] in
  let chain = [`Transpose []; `Bytes LE] in
  assert_raises (Zarr.Codecs.Invalid_transpose_order) (fun () -> Chain.create shape chain);
  assert_raises (Zarr.Codecs.Invalid_transpose_order) (fun () -> Chain.create shape [`Transpose [4; 0; 1]; `Bytes LE]))
;

"test sharding indexed codec" >:: (fun _ ->
  (* test missing chunk_shape field. *)
  decode_chain
    ~shape:[]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "end",
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (*test missing index_location field. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"chunk_shape": [5, 5, 5],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* test missing codecs field. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "end",
          "chunk_shape": [5, 5, 5],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* tests missing index_codecs field. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, 5, 5],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* tests incorrect value for index_location field. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "MIDDLE",
          "chunk_shape": [5, 5, 5],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* tests incorrect non-integer values for chunk_shape field. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, -5, 5.5],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* tests unspecified codecs field. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, 5, 5],
          "index_codecs": [],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* tests ill-formed codecs/index_codecs field. In this case, missing
     the required bytes->bytes codec. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, 5, 5],
          "index_codecs": [{"name": "crc32c"}],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* tests ill-formed codecs/index_codecs field. In this case, parsing
     an unsupported/unknown codec. *)
  decode_chain
    ~shape:[5; 5; 5]
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, 5, 5],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}},
             {"name": "UNKNOWN_BYTESTOBYTES_CODEC"}],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* test violation of index_codec invariant when it contains variable-sized codecs. *)
  List.iter
    (fun c ->
      decode_chain
        ~shape:[5; 5; 5]
        ~str:(Format.sprintf {|[
          {"name": "sharding_indexed",
           "configuration":
             {"index_location": "start",
              "chunk_shape": [5, 5, 5],
              "index_codecs":
                [{"name": "bytes", "configuration": {"endian": "big"}}, %s],
              "codecs":
                [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|} c)
        ~msg:"Must be exactly one array->bytes codec.")
    [{|{"name": "zstd", "configuration": {"level": 0, "checksum": false}}|}
    ;{|{"name": "gzip", "configuration": {"level": 1}}|}];

  let shape = [10; 15; 10] in
  let kind = Ndarray.Float64 in
  let cfg =
    {chunk_shape = [3; 5; 5]
    ;index_location = Start
    ;index_codecs = [`Transpose [0; 3; 1; 2]; `Bytes LE; `Crc32c]
    ;codecs = [`Bytes BE]}
  in
  let chain = [`ShardingIndexed cfg] in
  (*test failure for chunk shape not evenly dividing shard. *)
  assert_raises (Zarr.Codecs.Invalid_sharding_chunk_shape) (fun () -> Chain.create shape chain);
  (* test failure for chunk shape length not equal to dimensionality of shard.*)
  assert_raises
    (Zarr.Codecs.Invalid_sharding_chunk_shape)
    (fun () -> Chain.create shape @@ [`ShardingIndexed {cfg with chunk_shape = [5]}]);

  let chain = [`ShardingIndexed {cfg with chunk_shape = [5; 3; 5]}] in
  let c = Chain.create shape chain in
  let arr = Ndarray.create kind shape (-10.) in
  let encoded = Chain.encode c arr in
  assert_equal arr (Chain.decode c {shape; kind} encoded);

  (* test correctness of decoding nested sharding codecs.*)
  let str =
    {|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, 5, 5],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}],
          "codecs":
            [{"name": "sharding_indexed",
               "configuration":
                 {"index_location": "end",
                  "chunk_shape": [5, 5, 5],
                  "index_codecs":
                    [{"name": "bytes", "configuration": {"endian": "big"}}],
                  "codecs":
                    [{"name": "bytes", "configuration": {"endian": "big"}}]}}]}}]|}
    in
    let r = Chain.of_yojson shape @@ Yojson.Safe.from_string str in
    assert_bool "Encoding this nested sharding chain should not fail" @@ Result.is_ok r;
  (* test if decoding of indexed_codec with sharding for array->bytes fails.*)
  let str =
    {|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, 5, 5],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}],
          "index_codecs":
            [{"name": "sharding_indexed",
               "configuration":
                 {"index_location": "end",
                  "chunk_shape": [5, 5, 5, 1],
                  "index_codecs":
                    [{"name": "bytes", "configuration": {"endian": "big"}}],
                  "codecs":
                    [{"name": "bytes", "configuration": {"endian": "big"}}]}}]}}]|}
    in
    let r = Chain.of_yojson shape @@ Yojson.Safe.from_string str in
    assert_bool "Decoding of index_codec chain with sharding should fail" @@ Result.is_error r)
;


"test gzip codec" >:: (fun _ ->
  (* test wrong compression level *)
  decode_chain
    ~shape:[]
    ~str:{|[{"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "gzip", "configuration": {"level": -1}}]|}
    ~msg:"gzip codec is unsupported or has invalid configuration.";
  (* test incorrect configuration *)
  decode_chain
    ~shape:[]
    ~str:{|[{"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "gzip", "configuration": {"something": -1}}]|}
    ~msg:"gzip codec is unsupported or has invalid configuration.";

  (* test correct deserialization of gzip compression level *)
  let shape = [10; 15; 10] in
  List.iter
    (fun level ->
      let str =
        Format.sprintf
        {|[{"name": "bytes", "configuration": {"endian": "little"}},
           {"name": "gzip", "configuration": {"level": %d}}]|} level in
      let r = Chain.of_yojson shape @@ Yojson.Safe.from_string str in
      assert_bool "Encoding this chain should not fail" @@ Result.is_ok r)
    [0; 1; 2; 3; 4; 5; 6; 7; 8; 9];

  (* test encoding/decoding for various compression levels *)
  let kind = Ndarray.Complex64 in
  let fill_value = Complex.one in
  let arr = Ndarray.create kind shape fill_value in
  let chain = [`Bytes LE] in
  List.iter
    (fun level ->
      let c = Chain.create shape @@ chain @ [`Gzip level] in
      let encoded = Chain.encode c arr in
      assert_equal arr @@ Chain.decode c {shape; kind} encoded)
    [L0; L1; L2; L3; L4; L5; L6; L7; L8; L9])
;

"test zstd codec" >:: (fun _ ->
  (* test wrong compression level *)
  List.iter
    (fun l ->
      decode_chain
        ~shape:[]
        ~str:(Format.sprintf {|[{"name": "bytes", "configuration": {"endian": "little"}},
                {"name": "zstd", "configuration": {"level": %d, "checksum": false}}]|} l)
        ~msg:"zstd codec is unsupported or has invalid configuration.")
    [50; -500_000];

  (* test incorrect configuration *)
  decode_chain
    ~shape:[]
    ~str:{|[{"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "zstd", "configuration": {"something": -1}}]|}
    ~msg:"zstd codec is unsupported or has invalid configuration.";

  (* test correct deserialization of zstd compression level *)
  let shape = [10; 15; 10] in
  List.iter
    (fun level ->
      let str =
        Format.sprintf
        {|[{"name": "bytes", "configuration": {"endian": "little"}},
           {"name": "zstd", "configuration": {"level": %d, "checksum": false}}]|} level 
      in
      let r = Chain.of_yojson shape @@ Yojson.Safe.from_string str in
      assert_bool "Encoding this chain should not fail" @@ Result.is_ok r) [-131072; 0];

  (* test encoding/decoding for various compression levels *)
  let arr = Ndarray.create Int shape Int.max_int in
  List.iter
    (fun (level, checksum) ->
      let c = Chain.create shape [`Bytes LE; `Zstd (level, checksum)] in
      let encoded = Chain.encode c arr in
      assert_equal arr @@ Chain.decode c {shape; kind = Ndarray.Int} encoded)
    [(-131072, false); (-131072, true); (0, false); (0, true)])
;

"test bytes codec" >:: (fun _ ->
  let shape = [2; 2; 2] in
  (* test decoding of chain with invalid endianness name *)
  decode_chain
    ~shape
    ~str:{|[{"name": "bytes", "configuration": {"endian": "HUGE"}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* test decoding of chain with invalid configuration param. *)
  decode_chain
    ~shape
    ~str:{|[{"name": "bytes", "configuration": {"wrong": 5}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  
  (* test encoding/decoding of Char *)
  bytes_encode_decode {shape; kind = Ndarray.Char} '?';
  (* test encoding/decoding of Bool *)
  bytes_encode_decode {shape; kind = Ndarray.Bool} false;
  bytes_encode_decode {shape; kind = Ndarray.Bool} true;
  (* test encoding/decoding of int8 *)
  bytes_encode_decode {shape; kind = Ndarray.Int8} 0;
  (* test encoding/decoding of uint8 *)
  bytes_encode_decode {shape; kind = Ndarray.Uint8} 0;
  (* test encoding/decoding of int16 *)
  bytes_encode_decode {shape; kind = Ndarray.Int16} 0;
  (* test encoding/decoding of uint16 *)
  bytes_encode_decode {shape; kind = Ndarray.Uint16} 0;
  (* test encoding/decoding of int32 *)
  bytes_encode_decode {shape; kind = Ndarray.Int32} 0l;
  (* test encoding/decoding of int64 *)
  bytes_encode_decode {shape; kind = Ndarray.Int64} 0L;
  (* test encoding/decoding of float32 *)
  bytes_encode_decode {shape; kind = Ndarray.Float32} 0.0;
  (* test encoding/decoding of float64 *)
  bytes_encode_decode {shape; kind = Ndarray.Float64} 0.0;
  (* test encoding and decoding of Complex32 *)
  bytes_encode_decode {shape; kind = Ndarray.Complex32} Complex.zero;
  (* test encoding/decoding of complex64 *)
  bytes_encode_decode {shape; kind = Ndarray.Complex64} Complex.zero;
  (* test encoding/decoding of int *)
  bytes_encode_decode {shape; kind = Ndarray.Int} Int.max_int;
  (* test encoding/decoding of int *)
  bytes_encode_decode {shape; kind = Ndarray.Nativeint} Nativeint.max_int)
]
