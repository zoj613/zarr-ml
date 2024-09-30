(* this module implements a local filesystem zarr store that is backed by
   the Picos library for concurrent reads/writes. The main requirements
   is to implement the signature of Zarr.Types.IO. Here we use
   Zarr_sync's Deferred module to implement Zarr.Types.Deferred.

  To compile & run this example execute the command
    dune exec -- examples/picos_fs_store.exe
  in your shell at the root of this project. *)

module PU = Picos_io.Unix

module PicosFSStore : sig
  include Zarr.Storage.STORE with module Deferred = Zarr_sync.Deferred
  val create : ?perm:Unix.file_perm -> string -> t
end = struct
  
  module F = struct
    module Deferred = Zarr_sync.Deferred

    type t = {dirname : string; perm : PU.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos @@ String.length path - pos

    let key_to_fspath t key = Filename.concat t.dirname key

    let rec create_parent_dir fn perm =
      let parent_dir = Filename.dirname fn in
      try ignore @@ PU.stat parent_dir
      with PU.Unix_error (PU.ENOENT, _, _) ->
        create_parent_dir parent_dir perm;
        PU.mkdir parent_dir perm

    let size t key =
      match PU.openfile (key_to_fspath t key) [PU.O_RDONLY] t.perm with
      | exception Unix.Unix_error (Unix.ENOENT, "open", _) -> 0
      | fd ->
        Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
        PU.set_nonblock fd;
        PU.(fstat fd).st_size

    let get t key =
      let fd = PU.openfile (key_to_fspath t key) [PU.O_RDONLY] t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      PU.set_nonblock fd;
      let l = PU.(fstat fd).st_size in
      let buf = Bytes.create l in
      let _ = PU.read fd buf 0 l in
      Bytes.unsafe_to_string buf

    let get_partial_values t key ranges = 
      let fd = PU.openfile (key_to_fspath t key) [PU.O_RDONLY] t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      PU.set_nonblock fd;
      let tot = PU.(fstat fd).st_size in
      let l = List.fold_left
        (fun a (s, l) ->
          Option.fold ~none:(Int.max a (tot - s)) ~some:(Int.max a) l) 0 ranges in
      let buf = Bytes.create l in
      List.fold_right
        (fun (ofs, len) acc ->
          let _ = PU.lseek fd ofs PU.SEEK_SET in
          let size = Option.fold ~none:(tot - ofs) ~some:Fun.id len in
          let _ = PU.read fd buf 0 size in
          Bytes.sub_string buf 0 size :: acc) ranges []

    let set t key v =
      let p = key_to_fspath t key in
      create_parent_dir p t.perm;
      let fd = PU.openfile p PU.[O_WRONLY; O_TRUNC; O_CREAT] t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      PU.set_nonblock fd;
      ignore @@ PU.write_substring fd v 0 (String.length v)

    let set_partial_values t key ?(append=false) rvs =
      let flags = match append with
        | false -> PU.[O_WRONLY; O_CREAT] 
        | true -> PU.[O_APPEND; O_WRONLY; O_CREAT] 
      in
      let p = key_to_fspath t key in
      create_parent_dir p t.perm;
      let fd = PU.openfile p flags t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      rvs |> List.iter @@ fun (ofs, v) ->
      if append then ignore @@ PU.lseek fd 0 PU.SEEK_END
      else ignore @@ PU.lseek fd ofs PU.SEEK_SET;
      ignore @@ PU.write_substring fd v 0 (String.length v)

    let is_member t key =
      match PU.stat @@ key_to_fspath t key with
      | exception PU.Unix_error (PU.ENOENT, _, _) -> false
      | _ -> true

    let rec entries h acc =
      match PU.readdir h with
      | exception End_of_file -> acc
      | "." | ".." -> entries h acc
      | e -> entries h (e :: acc)

    let rec walk t acc dir =
      let h = PU.opendir dir in
      Fun.protect ~finally:(fun () -> PU.closedir h) @@ fun () ->
      List.fold_left
        (fun a x ->
          match Filename.concat dir x with
          | p when (PU.stat p).st_kind = PU.S_DIR -> walk t a p
          | p -> (fspath_to_key t p) :: a) acc @@ entries h []

    let list t = walk t [] (key_to_fspath t "")

    let list_prefix t prefix = walk t [] (key_to_fspath t prefix)

    let erase t key = PU.unlink @@ key_to_fspath t key

    let erase_prefix t pre = List.iter (erase t) @@ list_prefix t pre

    let list_dir t prefix =
      let dir = key_to_fspath t prefix in
      let h = PU.opendir dir in
      Fun.protect ~finally:(fun () -> PU.closedir h) @@ fun () ->
      entries h [] |> List.partition_map @@ fun x ->
      match Filename.concat dir x with
      | p when (PU.stat p).st_kind = PU.S_DIR -> Either.right @@ (fspath_to_key t p) ^ "/"
      | p -> Either.left @@ fspath_to_key t p

    let rename t k k' = PU.rename (key_to_fspath t k) (key_to_fspath t k')
  end

  let create ?(perm=0o700) dirname =
    Zarr.Util.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    F.{dirname = Zarr.Util.sanitize_dir dirname; perm}

  include Zarr.Storage.Make(F)
end

let _ =
  Picos_mux_random.run_on ~n_domains:1 @@ fun () ->
  let open Zarr in
  let open Zarr.Codecs in
  let open Zarr.Ndarray in
  let open Zarr.Indexing in

  let store = PicosFSStore.create "picosdata.zarr" in
  let gnode = Node.Group.of_path "/some/group" in
  PicosFSStore.Group.create store gnode;
  let anode = Node.Array.(gnode / "name") in
  let config =
    {chunk_shape = [|5; 3; 5|]
    ;codecs = [`Bytes LE; `Gzip L5]
    ;index_codecs = [`Bytes BE; `Crc32c]
    ;index_location = Start} in
  PicosFSStore.Array.create
    ~codecs:[`ShardingIndexed config]
    ~shape:[|100; 100; 50|]
    ~chunks:[|10; 15; 20|]
    Char '?' anode store;
  let slice = [|R [|0; 20|]; I 10; R [||]|] in
  let x = PicosFSStore.Array.read store anode slice Char in
  let x' = Zarr.Ndarray.map (fun _ -> Random.int 256 |> Char.chr) x in
  PicosFSStore.Array.write store anode slice x';
  let y = PicosFSStore.Array.read store anode slice Char in
  assert (equal x' y)
