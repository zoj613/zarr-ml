open Codecs_intf

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/v1.0.html *)
module TransposeCodec = struct
  let encoded_size input_size = input_size

  let encoded_repr ~order:o shape = Array.map (fun x -> shape.(x)) o

  let parse ~order:o shape =
    let o' = Array.copy o in
    Array.fast_sort Int.compare o';
    if Array.length o = 0
    || o' <> Array.(init (length o') Fun.id)
    || Array.(length o <> length shape)
    then raise Invalid_transpose_order else ()

  (* See https://github.com/owlbarn/owl/issues/671#issuecomment-2241761001 *)

  let encode o x = Ndarray.transpose ~axes:o x

  let decode o x =
    let inv_order = Array.(make (length o) 0) in
    Array.iteri (fun i x -> inv_order.(x) <- i) o;
    Ndarray.transpose ~axes:inv_order x

  let to_yojson order =
    let o = `List (Array.to_list @@ Array.map (fun x -> `Int x) order) in
    `Assoc
    [("name", `String "transpose")
    ;("configuration", `Assoc ["order", o])]

  let of_yojson chunk_shape x =
    match Yojson.Safe.Util.(member "configuration" x) with
    | `Assoc [("order", `List o)] ->
      Result.bind
        (List.fold_right
          (fun a acc ->
            Result.bind acc @@ fun k ->
            match a with
            | `Int i -> Ok (i :: k)
            | _ -> Error "transpose order values must be integers.") o (Ok []))
        (fun od ->
          let order = Array.of_list od in
          match parse ~order chunk_shape with
          | () -> Ok (`Transpose order)
          | exception Invalid_transpose_order -> Error "Invalid_transpose_order")
    | _ -> Error "Invalid transpose configuration."
end

module ArrayToArray = struct
  let parse t shp =
    match t with
    | `Transpose o -> TransposeCodec.parse ~order:o shp

  let encoded_size input_size = function
    | `Transpose _ -> TransposeCodec.encoded_size input_size

  let encoded_repr shape t =
    match t with
    | `Transpose o -> TransposeCodec.encoded_repr ~order:o shape

  let encode x = function
    | `Transpose order -> TransposeCodec.encode order x

  let decode t x =
    match t with
    | `Transpose order -> TransposeCodec.decode order x

  let to_yojson = function
    | `Transpose order -> TransposeCodec.to_yojson order

  let of_yojson cs x =
    match Util.get_name x with
    | "transpose" -> TransposeCodec.of_yojson cs x
    | s -> Error (Printf.sprintf "array->array codec %s not supported" s)
end
