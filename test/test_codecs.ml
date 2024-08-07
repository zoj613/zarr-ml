open OUnit2
open Zarr.Codecs

module Ndarray = Owl.Dense.Ndarray.Generic

let decode_chain ~str ~msg = 
  (match Chain.of_yojson @@ Yojson.Safe.from_string str with
  | Ok _ ->
    assert_failure
      "Impossible to decode an unsupported codec.";
  | Error s ->
    assert_equal ~printer:Fun.id msg s)

let bytes_encode_decode
  : type a b . (a, b) array_repr -> unit
  = fun decoded_repr ->
    List.iter
      (fun bytes_codec ->
        let chain = [bytes_codec] in
        let r = Chain.create decoded_repr chain in
        assert_bool
          "creating correct bytes codec chain should not fail." @@
          Result.is_ok r;
        let c = Result.get_ok r in
        let arr =
          Ndarray.create
            decoded_repr.kind decoded_repr.shape decoded_repr.fill_value in
        let encoded = Chain.encode c arr in
        assert_bool
          "encoding of well formed chain should not fail." @@
          Result.is_ok encoded;
        let decoded =
          Chain.decode c decoded_repr (Result.get_ok encoded) in
        assert_equal
          ~printer:Owl_pretty.dsnda_to_string
          arr
          (Result.get_ok decoded)) [`Bytes LE; `Bytes BE]

let tests = [
"test codec chain" >:: (fun _ ->
  let decoded_repr
    : (int, Bigarray.int16_signed_elt) array_repr =
    {shape = [|10; 15; 10|]
    ;kind = Bigarray.Int16_signed
    ;fill_value = 10}
  in
  let shard_cfg =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = End
    ;index_codecs = [`Bytes LE; `Crc32c]
    ;codecs = [`Transpose [|0; 1; 2|]; `Bytes BE; `Gzip L1]}
  in
  let chain  =
    [`Transpose [|2; 1; 0; 3|]; `ShardingIndexed shard_cfg; `Crc32c; `Gzip L9]
  in
  assert_bool
    "Chain with incorrect transpose dimension order cannot be created" @@
    Result.is_error @@
    Chain.create decoded_repr chain; 

  let chain  =
    [`Transpose [|2; 1; 0|]; `ShardingIndexed shard_cfg; `Bytes BE]
  in
  assert_bool
    "Chain with more than 1 array->bytes codec cannot be created" @@
    Result.is_error @@
    Chain.create decoded_repr chain; 

  let chain =
    [`Transpose [|2; 1; 0|]; `ShardingIndexed shard_cfg; `Crc32c; `Gzip L9]
  in
  let c = Chain.create decoded_repr chain in
  assert_bool "" @@ Result.is_ok c;
  let c = Result.get_ok c in
  let arr =
    Ndarray.create
      decoded_repr.kind
      decoded_repr.shape
      decoded_repr.fill_value
  in
  let enc = Chain.encode c arr in
  assert_bool
    "enc should be successfully encoded" @@
    Result.is_ok enc;
  let encoded = Result.get_ok enc in
  (match Chain.decode c decoded_repr encoded with
  | Ok v ->
    assert_bool "" @@ Ndarray.equal arr v;
  | Error _ ->
    assert_failure
      "Successfully encoded array should decode without fail");

  decode_chain ~str:"[]" ~msg:"No codec specified.";
  
  decode_chain
    ~str:{|[{"name": "gzip", "configuration": {"level": 1}}]|}
    ~msg:"Must be exactly one array->bytes codec.";

  decode_chain
    ~str:{|[{"name": "fake_codec"}, {"name": "bytes",
           "configuration": {"endian": "little"}}]|}
    ~msg:"fake_codec codec is unsupported or has invalid configuration.";

  let str = Chain.to_yojson c |> Yojson.Safe.to_string in
  (match Chain.of_yojson @@ Yojson.Safe.from_string str with
  | Ok v -> assert_equal v c;
  | Error _ ->
    assert_failure
      "a serialized chain should successfully deserialize"))
;

"test transpose codec" >:: (fun _ ->
  (* test decoding of chain with misspelled configuration name *)
  decode_chain
    ~str:{|[{"name": "transpose", "configuration": {"ordeR": [0, 1]}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding of chain with empty transpose order *)
  decode_chain
    ~str:{|[{"name": "transpose", "configuration": {"order": []}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding of chain with duplicated transpose order *)
  decode_chain
    ~str:{|[{"name": "transpose", "configuration": {"order": [0, 0]}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";
  (* test decoding with negative transpose dimensions. *)
  decode_chain
    ~str:{|[{"name": "transpose", "configuration": {"order": [-1]}},
           {"name": "bytes", "configuration": {"endian": "little"}}]|}
    ~msg:"transpose codec is unsupported or has invalid configuration.";

  (* test encoding transpose order bigger than an array's dimensionality. *)
  let str =
    {|[{"name": "transpose", "configuration": {"order": [0, 1, 2, 3]}},
      {"name": "bytes", "configuration": {"endian": "little"}}]|} in
  let arr =
    Ndarray.create Bigarray.Complex32 [|2; 2; 2|] Complex.zero in
  (match Chain.of_yojson @@ Yojson.Safe.from_string str with
  | Ok c ->
    assert_bool
      "Encoding this chain should not work" @@
      Result.is_error @@ Chain.encode c arr;
  | Error _ ->
    assert_failure
      "Decoding a well formed JSON codec field should not fail.");

  (* test decoding transpose order bigger/smaller than an array's dimensionality. *)
  let str' =
    {|[{"name": "transpose", "configuration": {"order": [0, 1, 2]}},
      {"name": "bytes", "configuration": {"endian": "little"}}]|} in
  (match Chain.of_yojson @@ Yojson.Safe.from_string str' with
  | Ok c ->
    let r = Chain.encode c arr in
    assert_bool "Encoding this chain should not fail" @@ Result.is_ok r;
    (* use config with too large order to decode encoded array.*)
    let cfg =
      Result.get_ok @@ Chain.of_yojson @@ Yojson.Safe.from_string str in
    let repr : (Complex.t, Bigarray.complex32_elt) array_repr =
      {shape = Ndarray.shape arr
      ;kind = Ndarray.kind arr
      ;fill_value =
        Ndarray.get arr @@ Array.make (Ndarray.num_dims arr) 0}
    in
    let r2 = Chain.decode cfg repr (Result.get_ok r) in
    assert_bool "This should never pass" @@ Result.is_error r2;
    (* use config with too small order to decode encoded array.*)
    let str =
      {|[{"name": "transpose", "configuration": {"order": [0, 1]}},
        {"name": "bytes", "configuration": {"endian": "little"}}]|} in
    let cfg =
      Result.get_ok @@ Chain.of_yojson @@ Yojson.Safe.from_string str in
    let r2 = Chain.decode cfg repr (Result.get_ok r) in
    assert_bool "This should never pass" @@ Result.is_error r2;
  | Error _ ->
    assert_failure
      "Decoding a well formed JSON codec field should not fail.");

  (* test encoding of chain with an empty or too big transpose order. *)
  let decoded_repr
    : (Complex.t, Bigarray.complex32_elt) array_repr =
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Complex32
    ;fill_value = Complex.zero}
  in
  let chain = [`Transpose [||]; `Bytes LE] in
  assert_bool
    "" @@ 
    Result.is_error @@
    Chain.create decoded_repr chain;
  assert_bool
    "Transpose codec with misisng dimensions should fail chain creation." @@ 
    Result.is_error @@
    Chain.create decoded_repr [`Transpose [|4; 0; 1|]; `Bytes LE])
;

"test sharding indexed codec" >:: (fun _ ->
  (* test missing chunk_shape field. *)
  decode_chain
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
  decode_chain
    ~str:{|[
      {"name": "sharding_indexed",
       "configuration":
         {"index_location": "start",
          "chunk_shape": [5, 5, 5],
          "index_codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}},
             {"name": "gzip", "configuration": {"level": 1}}],
          "codecs":
            [{"name": "bytes", "configuration": {"endian": "big"}}]}}]|}
    ~msg:"Must be exactly one array->bytes codec.";

  let decoded_repr
    : (float, Bigarray.float64_elt) array_repr =
    {shape = [|10; 15; 10|]
    ;kind = Bigarray.Float64
    ;fill_value = (-10.)}
  in
  let cfg =
    {chunk_shape = [|3; 5; 5|]
    ;index_location = Start
    ;index_codecs = [`Transpose [|0; 3; 1; 2|]; `Bytes LE; `Crc32c]
    ;codecs = [`Bytes BE]}
  in
  let chain = [`ShardingIndexed cfg] in
  (*test failure for chunk shape not evenly dividing shard. *)
  assert_bool
    "chunk shape must always evenly divide a shard" @@
    Result.is_error @@ Chain.create decoded_repr chain; 
  (* test failure for chunk shape length not equal to dimensionality of shard.*)
  assert_bool
    "chunk shape must have same size as shard dimensionality" @@
    Result.is_error @@ Chain.create decoded_repr @@ 
    [`ShardingIndexed {cfg with chunk_shape = [|5|]}];

  let chain = [`ShardingIndexed {cfg with chunk_shape = [|5; 3; 5|]}] in
  let c = Chain.create decoded_repr chain in
  assert_bool
    "Well formed shard config should not fail Chain creation" @@
    Result.is_ok c;
  let c = Result.get_ok c in

  let arr =
    Ndarray.create
      decoded_repr.kind
      decoded_repr.shape
      decoded_repr.fill_value
  in
  let enc = Chain.encode c arr in
  assert_bool
    "shard chain should be successfully encoded" @@
    Result.is_ok enc;
  let encoded = Result.get_ok enc in
  (match Chain.decode c decoded_repr encoded with
  | Ok v ->
    assert_equal ~printer:Owl_pretty.dsnda_to_string arr v;
  | Error _ ->
    assert_failure
      "Successfully encoded array should decode without fail");

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
    let r = Chain.of_yojson @@ Yojson.Safe.from_string str in
    assert_bool
      "Encoding this nested sharding chain should not fail" @@ Result.is_ok r;
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
    let r = Chain.of_yojson @@ Yojson.Safe.from_string str in
    assert_bool
      "Decoding of index_codec chain with sharding should fail" @@
      Result.is_error r)
;


"test gzip codec" >:: (fun _ ->
  (* test wrong compression level *)
  decode_chain
    ~str:{|[{"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "gzip", "configuration": {"level": -1}}]|}
    ~msg:"gzip codec is unsupported or has invalid configuration.";
  (* test incorrect configuration *)
  decode_chain
    ~str:{|[{"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "gzip", "configuration": {"something": -1}}]|}
    ~msg:"gzip codec is unsupported or has invalid configuration.";

  (* test correct deserialization of gzip compression level *)
  List.iter
    (fun level ->
      let str =
        Format.sprintf
        {|[{"name": "bytes", "configuration": {"endian": "little"}},
           {"name": "gzip", "configuration": {"level": %d}}]|} level 
      in
      let r = Chain.of_yojson @@ Yojson.Safe.from_string str in
      assert_bool
        "Encoding this chain should not fail" @@ Result.is_ok r)
    [0; 1; 2; 3; 4; 5; 6; 7; 8; 9];

  (* test encoding/decoding for various compression levels *)
  let decoded_repr
    : (Complex.t, Bigarray.complex64_elt) array_repr =
    {shape = [|10; 15; 10|]
    ;kind = Bigarray.Complex64
    ;fill_value = Complex.one}
  in
  let arr =
    Ndarray.create
      decoded_repr.kind
      decoded_repr.shape
      decoded_repr.fill_value
  in
  let chain = [`Bytes LE] in
  List.iter
    (fun level ->
      let c =
        Chain.create decoded_repr @@ chain @ [`Gzip level] in
      assert_bool
        "Creating `Gzip chain should not fail." @@
        Result.is_ok c;
      let c = Result.get_ok c in
      let enc = Chain.encode c arr in
      assert_bool
        "enc should be successfully encoded" @@
        Result.is_ok enc;
      let encoded = Result.get_ok enc in
      match Chain.decode c decoded_repr encoded with
      | Ok v ->
        assert_equal ~printer:Owl_pretty.dsnda_to_string arr v;
      | Error _ ->
        assert_failure
          "Successfully encoded array should decode without fail")
    [L0; L1; L2; L3; L4; L5; L6; L7; L8; L9])
;
"test bytes codec" >:: (fun _ ->
  (* test decoding of chain with invalid endianness name *)
  decode_chain
    ~str:{|[{"name": "bytes", "configuration": {"endian": "HUGE"}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  (* test decoding of chain with invalid configuration param. *)
  decode_chain
    ~str:{|[{"name": "bytes", "configuration": {"wrong": 5}}]|}
    ~msg:"Must be exactly one array->bytes codec.";
  
  (* test encoding/decoding of Char *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Char
    ;fill_value = '?'};

  (* test encoding/decoding of int8 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Int8_signed
    ;fill_value = 0};

  (* test encoding/decoding of uint8 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Int8_unsigned
    ;fill_value = 0};

  (* test encoding/decoding of int16 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Int16_signed
    ;fill_value = 0};

  (* test encoding/decoding of uint16 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Int16_unsigned
    ;fill_value = 0};

  (* test encoding/decoding of int32 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Int32
    ;fill_value = 0l};

  (* test encoding/decoding of int64 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Int64
    ;fill_value = 0L};

  (* test encoding/decoding of float32 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Float32
    ;fill_value = 0.0};

  (* test encoding/decoding of float64 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Float64
    ;fill_value = 0.0};

  (* test encoding and decoding of Complex32 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Complex32
    ;fill_value = Complex.zero};

  (* test encoding/decoding of complex64 *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Complex64
    ;fill_value = Complex.zero};

  (* test encoding/decoding of int *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Int
    ;fill_value = Int.max_int};

  (* test encoding/decoding of int *)
  bytes_encode_decode
    {shape = [|2; 2; 2|]
    ;kind = Bigarray.Nativeint
    ;fill_value = Nativeint.max_int})
]
