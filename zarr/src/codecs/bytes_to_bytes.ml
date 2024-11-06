open Codecs_intf
open Bytesrw

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/gzip/v1.0.html *)
module GzipCodec = struct
  let to_int = function
    | L0 -> 0 | L1 -> 1 | L2 -> 2 | L3 -> 3 | L4 -> 4
    | L5 -> 5 | L6 -> 6 | L7 -> 7 | L8 -> 8 | L9 -> 9

  let of_int = function
    | 0 -> Ok L0 | 1 -> Ok L1 | 2 -> Ok L2 | 3 -> Ok L3
    | 4 -> Ok L4 | 5 -> Ok L5 | 6 -> Ok L6 | 7 -> Ok L7
    | 8 -> Ok L8 | 9 -> Ok L9 | i ->
      Error (Printf.sprintf "Invalid Gzip level %d" i)

  let encode l x =
    let level = to_int l in
    let r = Bytes.Reader.of_string x in
    Bytes.Reader.to_string (Bytesrw_zlib.Gzip.compress_reads ~level () r)

  let decode x =
    let r = Bytes.Reader.of_string x in
    Bytes.Reader.to_string (Bytesrw_zlib.Gzip.decompress_reads () r)

  let to_yojson : compression_level -> Yojson.Safe.t = fun l ->
    `Assoc
    [("name", `String "gzip")
    ;("configuration", `Assoc ["level", `Int (to_int l)])]

  let of_yojson x =
    match Yojson.Safe.Util.(member "configuration" x |> to_assoc) with
    | [("level", `Int i)] -> Result.bind (of_int i) @@ fun l -> Ok (`Gzip l)
    | _ -> Error "Invalid Gzip configuration."
end

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/crc32c/v1.0.html *)
module Crc32cCodec = struct
  let encoded_size input_size = input_size + 4

  let encode x =
    let size = String.length x in
    let buf = Buffer.create size in
    Buffer.add_string buf x;
    let checksum = Checkseum.Crc32c.(default |> unsafe_digest_string x 0 size |> to_int32) in
    Buffer.add_int32_le buf checksum;
    Buffer.contents buf

  let decode x = String.sub x 0 (String.length x - 4)

  let to_yojson : Yojson.Safe.t = `Assoc [("name", `String "crc32c")]

  (* checks for validity of configuration are done via
     BytesToBytes.of_yojson so just return valid result.*)
  let of_yojson _ = Ok `Crc32c
end

(* https://github.com/zarr-developers/zarr-specs/pull/256 *)
module ZstdCodec = struct
  let min_clevel = -131072 and max_clevel = 22

  let parse_clevel l = if l < min_clevel || max_clevel < l then (raise Invalid_zstd_level)

  let encode clevel checksum x =
    let params = Bytesrw_zstd.Cctx_params.make ~checksum ~clevel () in
    let r = Bytes.Reader.of_string x in
    Bytes.Reader.to_string (Bytesrw_zstd.compress_reads ~params () r)

  let decode x =
    let r = Bytes.Reader.of_string x in
    Bytes.Reader.to_string (Bytesrw_zstd.decompress_reads () r)

  let to_yojson : int -> bool -> Yojson.Safe.t = fun l c ->
    `Assoc
    [("name", `String "zstd")
    ;("configuration", `Assoc [("level", `Int l); ("checksum", `Bool c)])]

  let of_yojson x =
    match Yojson.Safe.Util.(member "configuration" x) with
    | `Assoc [("level", `Int l); ("checksum", `Bool c)] ->
      (match parse_clevel l with
      | () -> Result.ok @@ `Zstd (l, c)
      | exception Invalid_zstd_level -> Error "Invalid_zstd_level")
    | _ -> Error "Invalid Zstd configuration."
end

module BytesToBytes = struct
  let encoded_size :
    int -> fixed_bytestobytes -> int
    = fun input_size -> function
    | `Crc32c -> Crc32cCodec.encoded_size input_size

  let parse = function
    | `Gzip _ | `Crc32c -> ()
    | `Zstd (l, _) -> ZstdCodec.parse_clevel l

  let encode x = function
    | `Gzip l -> GzipCodec.encode l x
    | `Crc32c -> Crc32cCodec.encode x
    | `Zstd (l, c) -> ZstdCodec.encode l c x

  let decode t x = match t with
    | `Gzip _ -> GzipCodec.decode x
    | `Crc32c -> Crc32cCodec.decode x
    | `Zstd _ -> ZstdCodec.decode x

  let to_yojson = function
    | `Gzip l -> GzipCodec.to_yojson l
    | `Crc32c -> Crc32cCodec.to_yojson 
    | `Zstd (l, c) -> ZstdCodec.to_yojson l c

  let of_yojson : Yojson.Safe.t -> (bytestobytes, string) result = fun x ->
    match Util.get_name x with
    | "gzip" -> GzipCodec.of_yojson x
    | "crc32c" -> Crc32cCodec.of_yojson x
    | "zstd" -> ZstdCodec.of_yojson x
    | s -> Error (Printf.sprintf "codec %s is not supported." s)
end
