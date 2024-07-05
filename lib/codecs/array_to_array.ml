module Ndarray = Owl.Dense.Ndarray.Generic

type dimension_order = int array [@@deriving show]

type array_to_array =
  | Transpose of dimension_order
  [@@deriving show]

type error =
  [ `Invalid_transpose_order of dimension_order * string ]

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/v1.0.html *)
module TransposeCodec = struct
  let compute_encoded_size input_size = input_size

  let compute_encoded_representation
    : type a b.
      int array ->
      (a, b) Util.array_repr ->
      ((a, b) Util.array_repr, [> error]) result
    = fun t decoded ->
      try
        let shape = Array.map (fun x -> decoded.shape.(x)) t in
        (* transpose codec should not lead to change in array size*)
        if Util.prod shape <> Util.prod decoded.shape then
          let msg =
            "transpose order leads to a change in encoded
            representation size, which is prohibited." in
          Result.error @@ `Invalid_transpose_order (t, msg)
        else
          Ok {decoded with shape}
      with
      | Invalid_argument _ ->
        let msg =
          "transpose order max element is larger than
          the decoded representation dimensionality." in
        Result.error @@ `Invalid_transpose_order (t, msg)


  let parse_order o =
    if Array.length o = 0 then
      let msg = "transpose order cannot be empty." in
      Result.error @@ `Invalid_transpose_order (o, msg)
    else
      let o' = Array.copy o in
      Array.fast_sort Int.compare o';
      if o' <> Array.init (Array.length o') Fun.id then
        let msg =
          "order must not have any repeated dimensions
          or negative values." in
        Result.error @@ `Invalid_transpose_order (o, msg)
      else
        Result.ok @@ Transpose o

  let parse
    : type a b.
      (a, b) Util.array_repr ->
      dimension_order ->
      (unit, [> error]) result
    = fun repr o ->
    ignore @@ parse_order o;
    let max = Array.length repr.shape in
    if Array.length o <> max then
      let msg =
        "Transpose order must have the same length
        as the decoded representation's number of dims." in
      Result.error @@ `Invalid_transpose_order (o, msg)
    else if not @@ Array.for_all (fun x -> x <= max) o then
      let msg =
        "Largest value of transpose order must not be larger than
        then dimensionality of the decoded representation." in
      Result.error @@ `Invalid_transpose_order (o, msg)
    else
      Ok ()

  let encode o x =
    try Ok (Ndarray.transpose ~axis:o x) with
    | Failure s -> Error (`Invalid_transpose_order (o, s))

  let decode o x =
    let inv_order = Array.(make (length o) 0) in
    Array.iteri (fun i x -> inv_order.(x) <- i) o;
    Ok (Ndarray.transpose ~axis:inv_order x)

  let to_yojson order =
    let o = 
      `List (Array.to_list @@ Array.map (fun x -> `Int x) order) in
    `Assoc
    [("name", `String "transpose")
    ;("configuration", `Assoc ["order", o])]

  let of_yojson x =
    let open Util.Result_syntax in
    match Yojson.Safe.Util.(member "configuration" x |> to_assoc) with
    | [("order", `List o)] ->
      if List.length o = 0 then
        Error "transpose order must not be empty."
      else
        List.fold_right
          (fun a acc ->
            acc >>= fun k ->
            match a with
            | `Int i
                when i >= 0  (* non-negative *)
                && not @@ List.mem i k (* unique *) ->
              Ok (i :: k)
            | _ ->
              let msg =
                "transpose order must only
                contain positive integers and unique values."
              in Error msg) o (Ok [])
        >>| fun o' -> Transpose (Array.of_list o')
    | _ -> Error "Invalid transpose configuration."
end

module ArrayToArray = struct
  let parse decoded_repr = function
    | Transpose o -> TransposeCodec.parse decoded_repr o

  let compute_encoded_size input_size = function
    | Transpose _ -> TransposeCodec.compute_encoded_size input_size

  let compute_encoded_representation
    : type a b.
      array_to_array ->
      (a, b) Util.array_repr ->
      ((a, b) Util.array_repr, [> error]) result
    = fun t repr ->
    match t with
    | Transpose o ->
      TransposeCodec.compute_encoded_representation o repr

  let encode t x =
    match t with
    | Transpose order -> TransposeCodec.encode order x

  let decode t x =
    match t with
    | Transpose order -> TransposeCodec.decode order x

  let to_yojson = function
    | Transpose order -> TransposeCodec.to_yojson order

  let of_yojson x =
    match Util.get_name x with
    | "transpose" -> TransposeCodec.of_yojson x
    | s -> Error ("array->array codec not supported: " ^ s)
end
