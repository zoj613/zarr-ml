open Codecs_intf

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/gzip/v1.0.html *)
module GzipCodec = struct
  let to_int = function
    | L0 -> 0 | L1 -> 1 | L2 -> 2 | L3 -> 3 | L4 -> 4
    | L5 -> 5 | L6 -> 6 | L7 -> 7 | L8 -> 8 | L9 -> 9

  let of_int = function
    | 0 -> Ok L0 | 1 -> Ok L1 | 2 -> Ok L2 | 3 -> Ok L3
    | 4 -> Ok L4 | 5 -> Ok L5 | 6 -> Ok L6 | 7 -> Ok L7
    | 8 -> Ok L8 | 9 -> Ok L9 | i ->
      Error ("Invalid Gzip compression level: " ^ (string_of_int i))

  let encode l x =
    Result.ok @@ Ezgzip.compress ~level:(to_int l) x

  let decode x = Ezgzip.decompress x

  let to_yojson l =
    `Assoc
    [("name", `String "gzip")
    ;("configuration", `Assoc ["level", `Int (to_int l)])]

  let of_yojson x =
    let open Util.Result_syntax in
    match Yojson.Safe.Util.(member "configuration" x |> to_assoc) with
    | [("level", `Int i)] ->
      of_int i >>| fun level -> `Gzip level
    | _ -> Error "Invalid Gzip configuration."
end

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/crc32c/v1.0.html *)
module Crc32cCodec = struct
  let compute_encoded_size input_size = input_size + 4

  let encode x =
    let size = String.length x in
    let buf = Buffer.create size in
    Buffer.add_string buf x;
    Buffer.add_int32_le buf @@
    Checkseum.Crc32c.(default |> digest_string x 0 size |> to_int32);
    Result.ok @@ Buffer.contents buf

  let decode x =
    Ok String.(length x - 4 |> sub x 0)

  let to_yojson =
    `Assoc [("name", `String "crc32c")]

  let of_yojson _ =
    (* checks for validity of configuration are done via
       BytesToBytes.of_yojson so just return valid result.*)
      Ok `Crc32c
end

module BytesToBytes = struct
  let compute_encoded_size :
    int -> fixed_bytestobytes -> int
    = fun input_size -> function
    | `Crc32c -> Crc32cCodec.compute_encoded_size input_size

  let encode t x = match t with
    | `Gzip l -> GzipCodec.encode l x
    | `Crc32c -> Crc32cCodec.encode x

  let decode t x = match t with
    | `Gzip _ -> GzipCodec.decode x
    | `Crc32c -> Crc32cCodec.decode x

  let to_yojson = function
    | `Gzip l -> GzipCodec.to_yojson l
    | `Crc32c -> Crc32cCodec.to_yojson 

  let of_yojson x =
    let open Util.Result_syntax in
    match Util.get_name x with
    | "gzip" ->
      GzipCodec.of_yojson x >>| fun gzip -> gzip
    | "crc32c" ->
      Crc32cCodec.of_yojson x >>| fun crc -> crc
    | s -> Error ("bytes->bytes codec not supported: " ^ s)
end
