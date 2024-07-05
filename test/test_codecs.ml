open OUnit2
open Zarr
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
  let chain = {a2a = []; a2b = Bytes Little; b2b = []} in
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
    (Result.get_ok decoded)

let tests = [
"test codec chain" >:: (fun _ ->
  let decoded_repr
    : (float, Bigarray.float32_elt) array_repr =
    {shape = [|10; 15; 10|]
    ;kind = Bigarray.Float32
    ;fill_value = (-10.)}
  in
  let shard_cfg =
    {chunk_shape = [|2; 5; 5|]
    ;index_location = End
    ;index_codecs = {a2a = []; a2b = Bytes Little; b2b = [Crc32c]}
    ;codecs = {a2a = [Transpose [|0; 1; 2|]]; a2b = Bytes Big; b2b = [Gzip L1]}}
  in
  let chain =
    {a2a = [Transpose [|2; 1; 0; 3|]]
    ;a2b = ShardingIndexed shard_cfg
    ;b2b = [Crc32c; Gzip L9]}
  in
  assert_bool
    "" @@
    Result.is_error @@
    Chain.create decoded_repr chain; 

  let chain = {chain with a2a = [Transpose [|2; 1; 0|]]} in
  let c = Chain.create decoded_repr chain in
  assert_bool "" @@ Result.is_ok c;
  let c = Result.get_ok c in
  assert_raises
    ~msg:"Encoded size cannot be computed for compression codecs."
    (Failure "Cannot compute encoded size of Gzip codec.")
    (fun () -> Chain.compute_encoded_size 0 c);

  let c' =
    Result.get_ok @@
    Chain.create decoded_repr {chain with b2b = [Crc32c]}
  in
  let init_size = 
    (Array.fold_left Int.mul 1 decoded_repr.shape) *
    Bigarray.kind_size_in_bytes decoded_repr.kind
  in
  assert_equal
    ~printer:string_of_int
    (init_size + 4 + 4) @@ (* 2 crc32c codecs *)
    Chain.compute_encoded_size init_size c';
    
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
  | Ok v ->
    assert_equal ~printer:Chain.show v c;
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
  let chain =
    {a2a = [Transpose [||]]; a2b = Bytes Little; b2b = []}
  in
  assert_bool
    "" @@ 
    Result.is_error @@
    Chain.create decoded_repr chain;
  assert_bool
    "" @@ 
    Result.is_error @@
    Chain.create decoded_repr
    {chain with a2a = [Transpose [|4; 0; 1|]]})
;

"test sharding indexed codec" >:: (fun _ ->
  let decoded_repr
    : (float, Bigarray.float64_elt) array_repr =
    {shape = [|10; 15; 10|]
    ;kind = Bigarray.Float64
    ;fill_value = (-10.)}
  in
  let cfg =
    {chunk_shape = [|3; 5; 5|]
    ;index_location = Start
    ;index_codecs = {a2a = []; a2b = Bytes Little; b2b = []}
    ;codecs = {a2a = []; a2b = Bytes Big; b2b = []}}
  in
  let chain =
    {a2a = []; a2b = ShardingIndexed cfg; b2b = []} in
  (*test failure for chunk shape not evenly dividing shard. *)
  assert_bool
    "chunk shape must always evenly divide a shard" @@
    Result.is_error @@ Chain.create decoded_repr chain; 
  (* test failure for chunk shape length not equal to dimensionality of shard.*)
  assert_bool
    "chunk shape must have same size as shard dimensionality" @@
    Result.is_error @@ Chain.create decoded_repr @@ 
    {a2a = []; a2b = ShardingIndexed {cfg with chunk_shape = [|5|]}; b2b = []};

  let chain =
    {a2a = []
    ;a2b = ShardingIndexed {cfg with chunk_shape = [|5; 5; 5|]}
    ;b2b = []}
  in
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
    assert_bool "" @@ Ndarray.equal arr v;
  | Error _ ->
    assert_failure
      "Successfully encoded array should decode without fail"))
;

"test gzip codec" >:: (fun _ ->
  decode_chain
    ~str:{|[{"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "gzip", "configuration": {"level": -1}}]|}
    ~msg:"gzip codec is unsupported or has invalid configuration.";

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
  let chain = {a2a = []; a2b = Bytes Little; b2b = []}
  in
  List.iter
    (fun level ->
      let c =
        Chain.create decoded_repr {chain with b2b = [Gzip level]} in
      assert_bool
        "Creating Gzip chain should not fail." @@
        Result.is_ok c;
      let c = Result.get_ok c in
      let enc = Chain.encode c arr in
      assert_bool
        "enc should be successfully encoded" @@
        Result.is_ok enc;
      let encoded = Result.get_ok enc in
      match Chain.decode c decoded_repr encoded with
      | Ok v ->
        assert_bool "" @@ Ndarray.equal arr v;
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
