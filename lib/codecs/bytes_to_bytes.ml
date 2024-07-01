module Ndarray = Owl.Dense.Ndarray.Generic

type compression_level =
  | L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

type bytes_to_bytes =
  | Crc32c
  | Gzip of compression_level

type error = 
  [ `Gzip of Ezgzip.error ]

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/gzip/v1.0.html *)
module GzipCodec = struct
  type config = {level : int} [@@deriving yojson]
  type gzip = config Util.ExtPoint.t [@@deriving yojson]

  let compute_encoded_size _ =
    failwith "Cannot compute encoded size of Gzip codec."

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
    gzip_to_yojson
      {name = "gzip"; configuration = {level = to_int l}}

  let of_yojson x =
    let open Util.Result_syntax in
    gzip_of_yojson x >>= fun gzip ->
    of_int gzip.configuration.level >>| fun level ->
    Gzip level
end

(* https://zarr-specs.readthedocs.io/en/latest/v3/codecs/crc32c/v1.0.html *)
module Crc32cCodec = struct
  type config = {name : string} [@@deriving yojson]

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
    config_to_yojson {name = "crc32c"}

  let of_yojson x =
    let open Util.Result_syntax in
    config_of_yojson x >>= fun _ -> Ok Crc32c
end

module BytesToBytes = struct
  let compute_encoded_size input_size = function
    | Gzip _ -> GzipCodec.compute_encoded_size input_size
    | Crc32c -> Crc32cCodec.compute_encoded_size input_size

  let encode t x = match t with
    | Gzip l -> GzipCodec.encode l x
    | Crc32c -> Crc32cCodec.encode x

  let decode t x = match t with
    | Gzip _ -> GzipCodec.decode x
    | Crc32c -> Crc32cCodec.decode x

  let to_yojson = function
    | Gzip l -> GzipCodec.to_yojson l
    | Crc32c -> Crc32cCodec.to_yojson 

  let of_yojson x =
    match Util.get_name x with
    | "gzip" -> GzipCodec.of_yojson x
    | "crc32c" -> Crc32cCodec.of_yojson x
    | s -> Error ("bytes->bytes codec not supported: " ^ s)
end
