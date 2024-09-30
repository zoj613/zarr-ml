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
    let rec extract_a2a (l, r) = match r with
      | #arraytoarray as x :: xs -> extract_a2a (l @ [x], xs)
      | xs -> (l, xs)
    in
    let extract_a2b = function
      | #fixed_arraytobytes as x :: xs -> (x, xs)
      | #variable_array_tobytes as x :: xs ->
        begin match x with
        | `ShardingIndexed cfg ->
          let codecs = create shape cfg.codecs in
          let index_codecs = create
            (Array.append shape [|2|])
            (cfg.index_codecs :> codec_chain) in
          (* coerse to a fixed codec internal_chain list type *)
          let b2b = List.filter_map (function
            | #fixed_bytestobytes as c -> Some c
            | _ -> None) index_codecs.b2b
          in
          let a2b = match index_codecs.a2b with
            | #fixed_arraytobytes as c -> c
            | _ -> raise Array_to_bytes_invariant 
          in
          let cfg' : internal_shard_config =
            {codecs
            ;chunk_shape = cfg.chunk_shape
            ;index_location = cfg.index_location
            ;index_codecs = {index_codecs with a2b; b2b}}
          in (`ShardingIndexed cfg', xs)
        end
      | _ -> raise Array_to_bytes_invariant
    in
    let rec extract_b2b (l, r) = match r with
      | #bytestobytes as x :: xs -> extract_b2b (l @ [x], xs)
      | xs -> (l, xs)
    in
    let a2a, rest = extract_a2a ([], cc) in
    let a2b, rest = extract_a2b rest in
    let b2b, other = extract_b2b ([], rest) in
    if List.compare_length_with other 0 <> 0 then raise Invalid_codec_ordering else
    ArrayToBytes.parse a2b @@
    (match a2a with
    | [] -> shape
    | x :: _ as xs -> 
      ArrayToArray.parse x shape;
      List.fold_left ArrayToArray.encoded_repr shape xs);
    List.iter BytesToBytes.parse b2b;
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
        Result.fold ~ok:(fun v -> v :: l, r) ~error:(fun _ -> l, c :: r) @@ f c)
        encoded ([], []) in
    let* codecs = match Yojson.Safe.Util.to_list x with
      | [] -> Error "No codec specified."
      | y -> Ok y
    in
    let* a2b, rest =
      match filter_partition (ArrayToBytes.of_yojson chunk_shape) codecs with
      | [x], rest -> Ok (x, rest)
      | _ -> Error "Must be exactly one array->bytes codec."
    in
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

  let partial_encode t f g bsize repr pairs fv =
    match t.a2b with
    | `ShardingIndexed c ->
      ShardingIndexedCodec.partial_encode c f g bsize repr pairs fv
    | `Bytes _ -> failwith "bytes codec does not support partial encoding." 

  let partial_decode t f s repr pairs fv =
    match t.a2b with
    | `ShardingIndexed c ->
      ShardingIndexedCodec.partial_decode c f s repr pairs fv
    | `Bytes _ -> failwith "bytes codec does not support partial decoding."
end
