include Bytes_to_bytes
include Array_to_array
include Array_to_bytes
open Util.Result_syntax

module Ndarray = Owl.Dense.Ndarray.Generic

type error =
  [ `Extension of string 
  | `Gzip of Ezgzip.error
  | `Transpose_order of int array * string
  | `CodecChain of string
  | `Sharding of int array * int array * string ]

type codec_chain =
  [ arraytoarray | arraytobytes | bytestobytes ] list

type internal_chain =
  {a2a : arraytoarray list
  ;a2b : arraytobytes
  ;b2b : bytestobytes list}
  (*;b2b_fixed : fixed_bytestobytes list
  ;b2b_variable : variable_bytestobytes list} *)

module Chain = struct
  type t = internal_chain

  let create : 
    type a b. (a, b) Util.array_repr -> codec_chain -> (t, [> error ]) result
    = fun repr cc ->
    let a2a, rest = List.partition_map (function
      | #arraytoarray as c -> Either.left c
      | #arraytobytes as c -> Either.right c
      | #bytestobytes as c -> Either.right c) cc
    in
    (match 
      List.partition_map (function
        | #arraytobytes as c -> Either.left c
        | #bytestobytes as c -> Either.right c) rest
    with
    | [x], rest -> Ok (x, rest)
    | _ ->
      Result.error @@ `CodecChain "Must be exactly one array->bytes codec.")
    >>= fun (a2b, b2b) ->
   (* let b2b_fixed, b2b_variable = List.partition_map (function
      | #fixed_bytestobytes as c -> Either.left c
      | #variable_bytestobytes as c -> Either.right c) rest
    in *)
    let ic = {a2a; a2b; b2b} in
    List.fold_left
      (fun acc c ->
         acc >>= fun r ->
         ArrayToArray.parse r c >>= fun () ->
         ArrayToArray.compute_encoded_representation c r) (Ok repr) ic.a2a
    >>= fun repr' ->
    ArrayToBytes.parse repr' ic.a2b >>| fun () -> ic

  let default : t =
    {a2a = []; a2b = ArrayToBytes.default; b2b = []}

  let encode :
    type a b. t -> (a, b) Ndarray.t -> (string, [> error ]) result
    = fun t x ->
    List.fold_left
      (fun acc c -> acc >>= ArrayToArray.encode c) (Ok x) t.a2a
    >>= fun y ->
    List.fold_left
      (fun acc c -> acc >>= BytesToBytes.encode c)
      (ArrayToBytes.encode y t.a2b) t.b2b

  let decode :
    type a b.
    t ->
    (a, b) Util.array_repr ->
    string ->
    ((a, b) Ndarray.t, [> error ]) result
    = fun t repr x ->
    (* compute the last encoded representation of array->array codec chain.
       This becomes the decoded representation of the array->bytes decode
       procedure. *)
    List.fold_left
      (fun acc c ->
        acc >>= ArrayToArray.compute_encoded_representation c)
      (Ok repr) t.a2a
    >>= fun repr' ->
    List.fold_right
      (fun c acc -> acc >>= BytesToBytes.decode c) t.b2b (Ok x)
    >>= fun y ->
    List.fold_right
      (fun c acc -> acc >>= ArrayToArray.decode c)
      t.a2a (ArrayToBytes.decode y repr' t.a2b)

  let ( = ) : t -> t -> bool = fun x y ->
    x.a2a = y.a2a && x.a2b = y.a2b && x.b2b = y.b2b

  let to_yojson : t -> Yojson.Safe.t = fun t ->
    `List
      (List.map ArrayToArray.to_yojson t.a2a @
      (ArrayToBytes.to_yojson t.a2b) ::
      List.map BytesToBytes.to_yojson t.b2b)

  let of_yojson : Yojson.Safe.t -> (t, string) result = fun x ->
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
    (match filter_partition ArrayToBytes.of_yojson codecs with
    | [x], rest -> Ok (x, rest)
    | _ -> Error "Must be exactly one array->bytes codec.")
    >>= fun (a2b, rest) ->
    let a2a, rest = filter_partition ArrayToArray.of_yojson rest in
    let b2b, rest = filter_partition BytesToBytes.of_yojson rest in
    match rest with
    | [] -> Ok ({a2a; a2b; b2b} : t)
    | x :: _ ->
      let msg =
        (Util.get_name x) ^
        " codec is unsupported or has invalid configuration." in
      Error msg
end
