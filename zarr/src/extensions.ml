module RegularGrid = struct
  exception Grid_shape_mismatch

  type t = int array

  let chunk_shape t = t

  let create ~array_shape cs =
    if Array.(length cs <> length array_shape) || Util.(max cs > max array_shape)
    then raise Grid_shape_mismatch else cs

  let ceildiv x y =
    Float.(to_int @@ ceil (of_int x /. of_int y))

  let floordiv x y =
    Float.(to_int @@ floor (of_int x /. of_int y))

  let grid_shape t array_shape =
    Array.map2 ceildiv array_shape t

  let index_coord_pair t coord =
    Array.map2
      (fun x y -> floordiv x y, Int.rem x y) coord t
    |> Array.split

  (* returns all chunk indices in this regular grid *)
  let indices t array_shape =
    grid_shape t array_shape
    |> Array.to_list
    |> List.map (fun x -> List.init x Fun.id)
    |> Ndarray.Indexing.cartesian_prod
    |> List.map Array.of_list

  let ( = ) x y = x = y

  let to_yojson t =
    let chunk_shape = 
      `List (Array.to_list @@ Array.map (fun x -> `Int x) t)
    in
    `Assoc
    [("name", `String "regular")
    ;("configuration", `Assoc [("chunk_shape", chunk_shape)])]
end

module ChunkKeyEncoding = struct
  type kind = Default | V2
  type t = {name : kind; sep : string; is_default : bool}

  let create = function
    | `Dot -> {name = Default; sep = "."; is_default = false}
    | `Slash -> {name = Default; sep = "/"; is_default = false}

  (* map a chunk coordinate index to a key. E.g, (2,3,1) maps to c/2/3/1 *)
  let encode {name; sep; _} index =
    let f i acc =
      string_of_int i :: acc
    in
    match name with
    | Default ->
      String.concat sep @@
      "c" :: Array.fold_right f index []
    | V2 ->
      if Array.length index = 0 
      then
        "0"
      else
        String.concat sep @@
        Array.fold_right f index []

  let ( = ) x y =
    x.name = y.name && x.sep = y.sep && x.is_default = y.is_default

  let to_yojson {name; sep; is_default} =
    let str =
      match name with
      | Default -> "default"
      | V2 -> "v2"
    in
    if is_default then
      `Assoc [("name", `String str)]
    else
      `Assoc
      [("name", `String str)
      ;("configuration", `Assoc [("separator", `String sep)])]

  let of_yojson x =
    match
      Util.get_name x, Yojson.Safe.Util.member "configuration" x
    with
    | "default", `Null ->
      Ok {name = Default; sep = "/"; is_default = true}
    | "default", `Assoc [("separator", `String "/")] ->
      Ok {name = Default; sep = "/"; is_default = false}
    | "default", `Assoc [("separator", `String ".")] ->
      Ok {name = Default; sep = "."; is_default = false}
    | "v2", `Null ->
      Ok {name = V2; sep = "."; is_default = true}
    | "v2", `Assoc [("separator", `String "/")] ->
      Ok {name = V2; sep = "/"; is_default = false}
    | "v2", `Assoc [("separator", `String ".")] ->
      Ok {name = V2; sep = "."; is_default = false}
    | _ -> Error "Invalid chunk key encoding configuration."
end

module Datatype = struct
  type t =
    | Char
    | Int8
    | Uint8
    | Int16
    | Uint16
    | Int32
    | Int64
    | Uint64
    | Float32
    | Float64
    | Complex32
    | Complex64
    | Int
    | Nativeint

  let ( = ) : t -> t -> bool = fun x y -> x = y

  let of_kind : type a. a Ndarray.dtype -> t = function
    | Ndarray.Char -> Char
    | Ndarray.Int8 -> Int8
    | Ndarray.Uint8 -> Uint8
    | Ndarray.Int16 -> Int16
    | Ndarray.Uint16 -> Uint16
    | Ndarray.Int32 -> Int32
    | Ndarray.Int64 -> Int64
    | Ndarray.Uint64 -> Uint64
    | Ndarray.Float32 -> Float32
    | Ndarray.Float64 -> Float64
    | Ndarray.Complex32 -> Complex32
    | Ndarray.Complex64 -> Complex64
    | Ndarray.Int -> Int
    | Ndarray.Nativeint -> Nativeint

  let to_yojson = function
    | Char -> `String "char"
    | Int8 -> `String "int8"
    | Uint8 -> `String "uint8"
    | Int16 -> `String "int16"
    | Uint16 -> `String "uint16"
    | Int32 -> `String "int32"
    | Int64 -> `String "int64"
    | Uint64 -> `String "uint64"
    | Float32 -> `String "float32"
    | Float64 -> `String "float64"
    | Complex32 -> `String "complex32"
    | Complex64 -> `String "complex64"
    | Int -> `String "int"
    | Nativeint -> `String "nativeint"

  let of_yojson = function
    | `String "char" -> Ok Char
    | `String "int8" -> Ok Int8
    | `String "uint8" -> Ok Uint8
    | `String "int16" -> Ok Int16
    | `String "uint16" -> Ok Uint16
    | `String "int32" -> Ok Int32
    | `String "int64" -> Ok Int64
    | `String "uint64" -> Ok Uint64
    | `String "float32" -> Ok Float32
    | `String "float64" -> Ok Float64
    | `String "complex32" -> Ok Complex32
    | `String "complex64" -> Ok Complex64
    | `String "int" -> Ok Int
    | `String "nativeint" -> Ok Nativeint
    | _ -> Error ("Unsupported metadata data_type")
end
