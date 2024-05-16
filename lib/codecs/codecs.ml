include Bytes_to_bytes
include Array_to_array
include Array_to_bytes
open Util.Result_syntax

module Ndarray = Owl.Dense.Ndarray.Generic

module Chain = struct
  type t = chain

  let create a2a a2b b2b = {a2a; a2b; b2b}
    (*let open Util.Result_syntax in
    match a2b with
    | Bytes _ -> {a2a; a2b; b2b}
    | ShardingIndexed c ->
      ArrayToBytes.parse_config c >>= fun () ->
      {a2a; a2b; b2b} *)

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
    List.fold_right
      (fun c acc -> acc >>= BytesToBytes.decode c) t.b2b (Ok x)
    >>= fun y ->
    List.fold_right
      (fun c acc -> acc >>= ArrayToArray.decode c)
      t.a2a (ArrayToBytes.decode y repr t.a2b)

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
    let codecs = Yojson.Safe.Util.to_list x
    in
    if List.length codecs = 0 then
      Error "No codec specified."
    else
      let a2b, rest = filter_partition ArrayToBytes.of_yojson codecs in
      if List.length a2b <> 1 then
        Error "Must be exactly one array->bytes codec."
      else
        let a2a, rest = filter_partition ArrayToArray.of_yojson rest in
        let b2b, rest = filter_partition BytesToBytes.of_yojson rest in
        if List.length rest <> 0 then
          Error ("Unsupported codec: " ^ (Util.get_name @@ List.hd rest))
        else
          Ok {a2a; a2b = List.hd a2b; b2b}
end
