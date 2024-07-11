type error =
  [ `Extension of string ]

module RegularGrid = struct
  type t = int array

  let chunk_shape t = t

  let create ~array_shape chunk_shape =
    match chunk_shape, array_shape with
    | c, a when Array.(length c <> length a) ->
      let msg = "grid chunk and array shape must have the same the length." in
      Result.error @@ `Extension msg
    | c, a when Util.(max c > max a) -> 
      let msg = "grid chunk dimension size must not be larger than array's." in
      Result.error @@ `Extension msg
    | c, _ -> Ok c

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

  let ( = ) x y = x = y

  let to_yojson t =
    let chunk_shape = 
      `List (Array.to_list @@ Array.map (fun x -> `Int x) t)
    in
    `Assoc
    [("name", `String "regular")
    ;("configuration", `Assoc [("chunk_shape", chunk_shape)])]

  let of_yojson x =
    let open Util.Result_syntax in
    match
      Util.get_name x,
      Yojson.Safe.Util.(member "configuration" x |> to_assoc)
    with
    | "regular", [("chunk_shape", `List xs)] ->
      List.fold_right
        (fun a acc ->
            acc >>= fun k ->
            match a with
            | `Int i when i > 0 -> Ok (i :: k)
            | _ ->
              let msg =
                "Regular grid chunk_shape must only contain positive integers."
              in
              Error msg) xs (Ok [])
      >>| Array.of_list
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
    | Float32
    | Float64
    | Complex32
    | Complex64
    | Int
    | Nativeint

  let ( = ) : t -> t -> bool = fun x y -> x = y

  let of_kind : type a b. (a, b) Bigarray.kind -> t = function
    | Bigarray.Char -> Char
    | Bigarray.Int8_signed -> Int8
    | Bigarray.Int8_unsigned -> Uint8
    | Bigarray.Int16_signed -> Int16
    | Bigarray.Int16_unsigned -> Uint16
    | Bigarray.Int32 -> Int32
    | Bigarray.Int64 -> Int64
    | Bigarray.Float32 -> Float32
    | Bigarray.Float64 -> Float64
    | Bigarray.Complex32 -> Complex32
    | Bigarray.Complex64 -> Complex64
    | Bigarray.Int -> Int
    | Bigarray.Nativeint -> Nativeint

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
    | `String "float32" -> Ok Float32
    | `String "float64" -> Ok Float64
    | `String "complex32" -> Ok Complex32
    | `String "complex64" -> Ok Complex64
    | `String "int" -> Ok Int
    | `String "nativeint" -> Ok Nativeint
    | _ -> Error ("Unsupported metadata data_type")
end

type tf_error =
  [ `Store_read of string
  | `Store_write of string ]

module type STF = sig
  type t
  val get : t -> string -> (string, [> tf_error]) result
  val set : t -> string -> string -> unit
  val erase : t -> string -> unit
end

module StorageTransformers = struct
  type transformer =
    | Identity
  type t = transformer list

  let default = [Identity]

  let deserialize x =
    match
      Util.get_name x,
      Yojson.Safe.Util.(member "configuration" x)
    with
    | "identity", `Null -> Ok Identity
    | _ ->
      Error "Unsupported storage transformer name or configuration."

  let of_yojson x =
    let open Util.Result_syntax in
    List.fold_right
      (fun x acc ->
        acc >>= fun l ->
        deserialize x >>| fun s ->
        s :: l)  (Yojson.Safe.Util.to_list x) (Ok [])

  let to_yojson x =
    `List
      (List.fold_right
        (fun x acc ->
          match x with
          | Identity -> acc) x [])

  let get
    (type a)
    (module M : STF with type t = a)
    (store : a)
    (transformers : t)
    (key : string)
    =
    let open Util.Result_syntax in
    M.get store key >>| fun raw ->
    snd @@
    List.fold_right
      (fun x (k, v) ->
        match x with
        | Identity -> (k, v)) transformers (key, raw)

  let set
    (type a)
    (module M : STF with type t = a)
    (store : a)
    (transformers : t)
    (key : string)
    (value : string)
    =
    let k', v' =
      List.fold_left
        (fun (k, v) -> function
          | Identity -> (k, v)) (key, value) transformers
    in
    M.set store k' v'

  let erase
    (type a)
    (module M : STF with type t = a)
    (store : a)
    (transformers : t)
    (key : string)
    =
    M.erase store @@
      List.fold_left
        (fun k -> function
          | Identity -> k) key transformers
end
