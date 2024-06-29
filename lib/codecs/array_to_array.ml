module Ndarray = Owl.Dense.Ndarray.Generic

type dimension_order = int array

type array_to_array =
  | Transpose of dimension_order

type error =
  [ `Invalid_transpose_order of dimension_order * string ]

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/v1.0.html *)
module TransposeCodec = struct
  type config = {order : int array} [@@deriving yojson]
  type transpose = config Util.ext_point  [@@deriving yojson]

  let compute_encoded_size input_size = input_size

  let compute_encoded_representation
    : type a b.
      int array ->
      (a, b) Util.array_repr ->
      (a, b) Util.array_repr
    = fun t decoded ->
      let shape = Array.(make (length decoded.shape) 0) in
      Array.iteri (fun i x -> shape.(i) <- decoded.shape.(x)) t;
      {decoded with shape}

  let parse_order o =
    let o' = Array.copy o in
    Array.fast_sort Int.compare o';
    if o' <> Array.init (Array.length o') Fun.id then
      Error (`Invalid_transpose_order (o, ""))
    else
      Result.ok @@ Transpose o

  let parse
    : type a b.
      (a, b) Util.array_repr ->
      dimension_order ->
      (unit, [> error]) result
    = fun repr o ->
    ignore @@ parse_order o;
    if Array.length o <> Array.length repr.shape then
      let msg =
        "Transpose order must have the same length
        as the chunk it encodes/decodes." in
      Result.error @@ `Invalid_transpose_order (o, msg)
    else
      Ok ()

  let encode o x =
    try Ok (Ndarray.transpose ~axis:o x) with
    | Failure s -> Error (`Invalid_transpose_order (o, s))

  let decode o x =
    let inv_order = Array.(make (length o) 0) in
    Array.iteri (fun i x -> inv_order.(x) <- i) o;
    try Ok (Ndarray.transpose ~axis:inv_order x) with
    | Failure s -> Error (`Invalid_transpose_order (o, s))

  let to_yojson order =
    transpose_to_yojson
      {name = "transpose"; configuration = {order}}

  let of_yojson x =
    let open Util.Result_syntax in
    transpose_of_yojson x >>= fun trans ->
    parse_order trans.configuration.order
    >>? fun (`Invalid_transpose_order _) -> "Invalid transpose order"
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
      (a, b) Util.array_repr
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
