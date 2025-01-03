module RegularGrid = struct
  exception Grid_shape_mismatch
  type t = int list
  let chunk_shape : t -> int list = Fun.id
  let ceildiv x y = Float.(to_int @@ ceil (of_int x /. of_int y))
  let floordiv x y = Float.(to_int @@ floor (of_int x /. of_int y))
  let grid_shape t array_shape = List.map2 ceildiv array_shape t
  let index_coord_pair t coord = (List.map2 floordiv coord t, List.map2 Int.rem coord t)
  let ( = ) x y = List.equal Int.equal x y
  let max = List.fold_left Int.max Int.min_int

  let create ~array_shape chunk_shape =
    if List.(length chunk_shape <> length array_shape) || (max chunk_shape > max array_shape)
    then raise Grid_shape_mismatch else chunk_shape

  (* returns all chunk indices in this regular grid *)
  let indices t array_shape =
    let lol = List.map (fun x -> List.init x Fun.id) (grid_shape t array_shape) in
    Ndarray.Indexing.cartesian_prod lol

  let to_yojson (g : t) : Yojson.Safe.t =
    let name = ("name", `String "regular") in
    `Assoc [name; ("configuration", `Assoc [("chunk_shape", `List (List.map (fun x -> `Int x) g))])]

  let add (x : Yojson.Safe.t) acc = match x with
    | `Int i when i > 0 -> Result.map (List.cons i) acc
    | _ -> Error "chunk_shape must only contain positive ints."

  let of_yojson (array_shape: int list) (x : Yojson.Safe.t) = match x with
    | `Assoc ["name", `String "regular"; "configuration", `Assoc ["chunk_shape", `List l]] ->
      begin try Result.map (create ~array_shape) (List.fold_right add l (Ok []))
      with Grid_shape_mismatch -> Error "grid shape mismatch." end
    | `Null -> Error "array metadata must contain a chunk_grid field." 
    | _ -> Error "Invalid Chunk grid name or configuration."
end

module ChunkKeyEncoding = struct
  type kind = Default | V2
  type t = {name : kind; sep : string; is_default : bool}

  let create = function
    | `Dot -> {name = Default; sep = "."; is_default = false}
    | `Slash -> {name = Default; sep = "/"; is_default = false}

  (* map a chunk coordinate index to a key. E.g, (2,3,1) maps to c/2/3/1 *)
  let encode {name; sep; _} index =
    let xs = List.fold_right (fun i acc -> string_of_int i :: acc) index [] in
    match name with
    | Default -> String.concat sep ("c" :: xs)
    | V2 -> if List.length index = 0 then "0" else String.concat sep xs

  let ( = ) x y = Bool.equal x.is_default y.is_default && String.equal x.sep y.sep && x.name = y.name

  let to_yojson : t -> Yojson.Safe.t = fun {name; sep; is_default} ->
    let str = match name with
      | Default -> "default"
      | V2 -> "v2"
    in
    if is_default then `Assoc [("name", `String str)] else
    `Assoc [("name", `String str); ("configuration", `Assoc [("separator", `String sep)])]

  let of_yojson : Yojson.Safe.t -> (t, string) result = function
    | `Assoc [("name", `String "v2")] -> Ok {name = V2; sep = "."; is_default = true}
    | `Assoc [("name", `String "v2"); ("configuration", `Assoc [("separator", `String ("/" as slash))])] ->
      Ok {name = V2; sep = slash; is_default = false}
    | `Assoc [("name", `String "v2"); ("configuration", `Assoc [("separator", `String ("." as dot))])] ->
      Ok {name = V2; sep = dot; is_default = false}
    | `Assoc [("name", `String "default")] -> Ok {name = Default; sep = "/"; is_default = true}
    | `Assoc [("name", `String "default"); ("configuration", `Assoc [("separator", `String ("/" as slash))])] ->
      Ok {name = Default; sep = slash; is_default = false}
    | `Assoc [("name", `String "default"); ("configuration", `Assoc [("separator", `String ("." as dot))])] ->
      Ok {name = Default; sep = dot; is_default = false}
    | `Null -> Error "array metadata must contain a chunk_key_encoding field."
    | _ -> Error "Invalid chunk key encoding configuration."
end

module Datatype = struct
  type t =
    | Char
    | Bool
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
    | Ndarray.Bool -> Bool
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
    | Bool -> `String "bool"
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
    | `String "bool" -> Ok Bool
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
    | `Null -> Error "array metadata must contain a data_type field."
    | _ -> Error "Unsupported metadata data_type"
end
