(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/v1.0.html *)
module TransposeCodec = struct
  let compute_encoded_size input_size = input_size

  let compute_encoded_representation ~order:o shape =
    Array.map (fun x -> shape.(x)) o

  let parse ~order:o shape =
    let o' = Array.copy o in
    Array.fast_sort Int.compare o';
    if Array.length o = 0 then failwith "transpose order cannot be empty."
    else if o' <> Array.init (Array.length o') Fun.id then
      failwith "transpose must have unique non-negative values."
    else if Array.(length o <> length shape) then
      failwith "transpose order and chunk shape mismatch."
    else ()

  module A = Owl.Dense.Ndarray.Any
  module N = Owl.Dense.Ndarray.Generic
  (* See https://github.com/owlbarn/owl/issues/671#issuecomment-2241761001 *)
  let transpose ?axis x =
    let y = A.transpose ?axis @@ A.init_nd (N.shape x) @@ N.get x in
    N.init_nd (N.kind x) (A.shape y) @@ A.get y

  let encode o x = transpose ~axis:o x

  let decode o x =
    let inv_order = Array.(make (length o) 0) in
    Array.iteri (fun i x -> inv_order.(x) <- i) o;
    transpose ~axis:inv_order x

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
          try parse ~order chunk_shape; Ok (`Transpose order) with
          | Failure s -> Error s)
    | _ -> Error "Invalid transpose configuration."
end

module ArrayToArray = struct
  let parse t shp =
    match t with
    | `Transpose o -> TransposeCodec.parse ~order:o shp

  let compute_encoded_size input_size = function
    | `Transpose _ -> TransposeCodec.compute_encoded_size input_size

  let compute_encoded_representation shape t =
    match t with
    | `Transpose o ->
      TransposeCodec.compute_encoded_representation ~order:o shape

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
