include Bytes_to_bytes
include Array_to_array
include Array_to_bytes
open Util.Result_syntax

module Ndarray = Owl.Dense.Ndarray.Generic

module Chain = struct
  type t = chain

  let pp = pp_chain

  let show = show_chain

  let create repr {a2a; a2b; b2b} =
    List.fold_left
      (fun acc c ->
         acc >>= fun r ->
         ArrayToArray.parse r c >>= fun () ->
         ArrayToArray.compute_encoded_representation c r) (Ok repr) a2a
    >>= fun repr' ->
    ArrayToBytes.parse repr' a2b >>| fun () ->
    {a2a; a2b; b2b}

  let default =
    {a2a = []; a2b = ArrayToBytes.default; b2b = []}

  let compute_encoded_size input_size t =
    List.fold_left BytesToBytes.compute_encoded_size
      (ArrayToBytes.compute_encoded_size
         (List.fold_left ArrayToArray.compute_encoded_size
            input_size t.a2a) t.a2b) t.b2b

  let encode t x = 
    List.fold_left
      (fun acc c -> acc >>= ArrayToArray.encode c) (Ok x) t.a2a
    >>= fun y ->
    List.fold_left
      (fun acc c -> acc >>= BytesToBytes.encode c)
      (ArrayToBytes.encode y t.a2b) t.b2b

  let decode t repr x =
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

  let equal x y =
    x.a2a = y.a2a && x.a2b = y.a2b && x.b2b = y.b2b

  let to_yojson t =
    [%to_yojson: Yojson.Safe.t list] @@ 
    List.map ArrayToArray.to_yojson t.a2a @
    (ArrayToBytes.to_yojson t.a2b) ::
    List.map BytesToBytes.to_yojson t.b2b

  let of_yojson x =
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
    | [] -> Ok {a2a; a2b; b2b}
    | x :: _ ->
      let msg =
        (Util.get_name x) ^
        " codec is unsupported or has invalid configuration." in
      Error msg
end
