open Bytes_to_bytes
open Array_to_array
open Array_to_bytes

include Codecs_intf

type fixed_arraytobytes =
  [ `Bytes of endianness ]

type variable_array_tobytes =
  [ `ShardingIndexed of shard_config ]

and shard_config =
  {chunk_shape : int array
  ;codecs :
    [ arraytoarray
    | fixed_arraytobytes
    | `ShardingIndexed of shard_config
    | bytestobytes ] list
  ;index_codecs :
    [ arraytoarray | fixed_arraytobytes | fixed_bytestobytes ] list
  ;index_location : loc}

type array_tobytes =
  [ fixed_arraytobytes | variable_array_tobytes ]

type codec_chain =
  [ arraytoarray | array_tobytes | bytestobytes ] list

module Chain = struct
  type t = (arraytobytes, bytestobytes) internal_chain

  let rec create : int array -> codec_chain -> t = fun shape cc ->
    let a2a, rest =
      List.partition_map
        (function
        | #arraytoarray as c -> Either.left c
        | #array_tobytes as c -> Either.right c
        | #bytestobytes as c -> Either.right c) cc
    in
    let result =
      List.fold_right
        (fun c (l, r) ->
          match c with
          | #bytestobytes as c -> l, c :: r
          | #fixed_arraytobytes as c -> c :: l, r
          | `ShardingIndexed cfg ->
            let codecs = create shape cfg.codecs in
            let index_codecs =
              create
                (Array.append shape [|2|])
                (cfg.index_codecs :> codec_chain) in
            (* coerse to a fixed codec internal_chain list type *)
            let b2b =
              fst @@
              List.partition_map
                (function
                | #fixed_bytestobytes as c -> Either.left c
                | c -> Either.right c) index_codecs.b2b
            in
            let a2b =
              List.hd @@
              fst @@
              List.partition_map
                (function
                | #fixed_arraytobytes as c -> Either.left c
                | c -> Either.right c) [index_codecs.a2b]
            in
            let cfg' : internal_shard_config =
              {codecs
              ;chunk_shape = cfg.chunk_shape
              ;index_location = cfg.index_location
              ;index_codecs = {index_codecs with a2b; b2b}}
            in `ShardingIndexed cfg' :: l, r) rest ([], [])
    in
    let a2b, b2b =
      match result with
      | [x], r -> x, r
      | _ -> failwith "Must be exactly one array->bytes codec."
    in
    ArrayToBytes.parse a2b @@
    (match a2a with
    | [] -> shape
    | l -> 
      ArrayToArray.parse (List.hd l) shape;
      List.fold_left ArrayToArray.encoded_repr shape l);
    {a2a; a2b; b2b}

  let encode t x =
      List.fold_left
        BytesToBytes.encode
        (ArrayToBytes.encode t.a2b @@
          List.fold_left ArrayToArray.encode x t.a2a) t.b2b

  let decode t repr x =
    let shape = List.fold_left ArrayToArray.encoded_repr repr.shape t.a2a in
    List.fold_right
      ArrayToArray.decode t.a2a @@
      ArrayToBytes.decode t.a2b {repr with shape} @@
      List.fold_right BytesToBytes.decode t.b2b x

  let ( = ) x y =
    x.a2a = y.a2a && x.a2b = y.a2b && x.b2b = y.b2b

  let to_yojson t =
    `List
      (List.map ArrayToArray.to_yojson t.a2a @
      (ArrayToBytes.to_yojson t.a2b) ::
      List.map BytesToBytes.to_yojson t.b2b)

  let of_yojson chunk_shape x =
    let open Util.Result_syntax in
    let filter_partition f encoded =
      List.fold_right (fun c (l, r) ->
        match f c with
        | Ok v -> v :: l, r
        | Error _ -> l, c :: r) encoded ([], [])
    in
    (match Yojson.Safe.Util.to_list x with
    | [] -> Error "No codec specified."
    | y -> Ok y)
    >>= fun codecs ->
    (match filter_partition (ArrayToBytes.of_yojson chunk_shape) codecs with
    | [x], rest -> Ok (x, rest)
    | _ -> Error "Must be exactly one array->bytes codec.")
    >>= fun (a2b, rest) ->
    let a2a, rest = filter_partition (ArrayToArray.of_yojson chunk_shape) rest in
    let b2b, rest = filter_partition BytesToBytes.of_yojson rest in
    match rest with
    | [] -> Ok {a2a; a2b; b2b}
    | x :: _ ->
      Result.error @@
      Printf.sprintf
        "%s codec is unsupported or has invalid configuration." @@ Util.get_name x
end

module Make (Io : Types.IO) = struct
  module ShardingIndexedCodec = Array_to_bytes.Make(Io)

  let is_just_sharding = function
    | {a2a = []; a2b = `ShardingIndexed _; b2b = []} -> true
    | _ -> false

  let partial_encode t f g bsize repr pairs =
    match t.a2b with
    | `ShardingIndexed c ->
      ShardingIndexedCodec.partial_encode c f g bsize repr pairs
    | `Bytes _ -> failwith "bytes codec does not support partial encoding." 

  let partial_decode t f s repr pairs =
    match t.a2b with
    | `ShardingIndexed c ->
      ShardingIndexedCodec.partial_decode c f s repr pairs
    | `Bytes _ -> failwith "bytes codec does not support partial decoding."
end
