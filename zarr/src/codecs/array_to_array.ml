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

  let encode o x = Ndarray.transpose ~axes:o x

  let decode o x =
    let inv_order = Array.(make (length o) 0) in
    Array.iteri (fun i x -> inv_order.(x) <- i) o;
    Ndarray.transpose ~axes:inv_order x

  let to_yojson : int array -> Yojson.Safe.t = fun order ->
    let o = `List (List.map (fun x -> `Int x) @@ Array.to_list order) in
    `Assoc
    [("name", `String "transpose")
    ;("configuration", `Assoc ["order", o])]

  let of_yojson :
    int array -> Yojson.Safe.t -> ([`Transpose of int array], string) result
    = fun chunk_shape x ->
    let add_as_int v acc = match v with
      | `Int i -> Ok (i :: acc)
      | _ -> Error "transpose order values must be integers."
    in
    let to_codec ~chunk_shape o =
      let o' = Array.of_list o in
      match parse ~order:o' chunk_shape with
      | exception Invalid_transpose_order -> Error "Invalid_transpose_order"
      | () -> Ok (`Transpose o')
    in
    match Yojson.Safe.Util.(member "configuration" x) with
    | `Assoc [("order", `List o)] ->
      let accumulate a acc = Result.bind acc (add_as_int a) in
      Result.bind (List.fold_right accumulate o (Ok [])) (to_codec ~chunk_shape)
    | _ -> Error "Invalid transpose configuration."
end

module ArrayToArray = struct
  let parse t shp = match t with
    | `Transpose o -> TransposeCodec.parse ~order:o shp

  let encoded_size input_size = function
    | `Transpose _ -> TransposeCodec.encoded_size input_size

  let encoded_repr shape t = match t with
    | `Transpose o -> TransposeCodec.encoded_repr ~order:o shape

  let encode x = function
    | `Transpose order -> TransposeCodec.encode order x

  let decode t x = match t with
    | `Transpose order -> TransposeCodec.decode order x

  let to_yojson = function
    | `Transpose order -> TransposeCodec.to_yojson order

  let of_yojson cs x = match Util.get_name x with
    | "transpose" -> TransposeCodec.of_yojson cs x
    | s -> Error (Printf.sprintf "array->array codec %s not supported" s)
end
