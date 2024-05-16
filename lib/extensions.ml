open Util.Result_syntax

module RegularGrid = struct
  type config =
    {chunk_shape : int array} [@@deriving yojson]
  type chunk_grid =
    config Util.ext_point [@@deriving yojson]
  type t = int array

  let chunk_shape t = t

  let create chunk_shape = chunk_shape

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
    |> Util.Indexing.cartesian_prod
    |> List.map Array.of_list

  let to_yojson t =
    chunk_grid_to_yojson
      {name = "regular"; configuration = {chunk_shape = t}}

  let of_yojson x =
    chunk_grid_of_yojson x >>= fun y ->
    if y.name <> "regular" then
      Error ("chunk grid name should be 'regular' not: " ^ y.name)
    else
      Ok y.configuration.chunk_shape
end

type separator = Dot | Slash

module ChunkKeyEncoding = struct
  type encoding = Default | V2
  type config = {separator : string} [@@deriving yojson]
  type key_encoding = config Util.ext_point [@@deriving yojson]
  type t = {encoding : encoding; sep : string}

  let create = function
    | Dot -> {encoding = Default; sep = "."}
    | Slash -> {encoding = Default; sep = "/"}

  (* map a chunk coordinate index to a key. E.g, (2,3,1) maps to c/2/3/1 *)
  let encode t index =
    let f i acc =
      string_of_int i :: acc
    in
    match t.encoding with
    | Default ->
      String.concat t.sep @@
      "c" :: Array.fold_right f index []
    | V2 ->
      if Array.length index = 0 
      then
        "0"
      else
        String.concat t.sep @@
        Array.fold_right f index []

  let to_yojson t =
    match t.encoding with
    | Default ->
      key_encoding_to_yojson
        {name = "default"; configuration = {separator = t.sep}}
    | V2 ->
      key_encoding_to_yojson
        {name = "v2"; configuration = {separator = t.sep}}

  let of_yojson x =
    key_encoding_of_yojson x >>= fun y ->
    match y with
    | {name = "default"; configuration = {separator = "/"}} ->
      Ok {encoding = Default; sep = "/"}
    | {name = "default"; configuration = {separator = "."}} ->
      Ok {encoding = Default; sep = "."}
    | {name = "v2"; configuration = {separator = "."}} ->
      Ok {encoding = V2; sep = "."}
    | {name = "v2"; configuration = {separator = "/"}} ->
      Ok {encoding = V2; sep = "/"}
    | {name = e; configuration = {separator = s}} ->
      Error ("Unsupported chunk key configuration: " ^ e ^ ", " ^ s)
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
    | Float32
    | Float64
    | Complex32
    | Complex64

  let to_yojson = function
    | Char -> `String "char"
    | Int8 -> `String "int8"
    | Uint8 -> `String "uint8"
    | Int16 -> `String "int16"
    | Uint16 -> `String "uint16"
    | Int32 -> `String "int32"
    | Int64 -> `String "int64"
    | Float32 -> `String "float32"
    | Float64 -> `String "float64"
    | Complex32 -> `String "complex32"
    | Complex64 -> `String "complex64"

  let of_yojson = function
    | `String "char" -> Ok Char
    | `String "int8" -> Ok Int8
    | `String "uint8" -> Ok Uint8
    | `String "int16" -> Ok Int16
    | `String "uint16" -> Ok Uint16
    | `String "int32" -> Ok Int32
    | `String "int64" -> Ok Int64
    | `String "float32" -> Ok Float32
    | `String "float64" -> Ok Float64
    | `String "complex32" -> Ok Complex32
    | `String "complex64" -> Ok Complex64
    | _ -> Error ("Unsupported metadata data_type")
end
