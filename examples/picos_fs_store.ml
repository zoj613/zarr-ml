(* this module implements a local filesystem zarr store that is backed by
   the Picos library for concurrent reads/writes. The main requirements
   is to implement the signature of Zarr.Types.Store.

  To compile & run this example execute the command
    dune exec -- examples/picos_fs_store.exe
  in your shell at the root of this project. *)

module PU = Picos_io.Unix
module IO = Zarr_sync.Storage.IO

type error = [ `Read of string | `Write of string ]

module PicosFSStore : sig
  include Zarr.Storage.S with type 'a io := 'a and type error = error
  val create : ?perm:Unix.file_perm -> string -> t
end = struct
  
  module Store = struct
    open IO.Syntax
    type t = {dirname : string; perm : PU.file_perm}
    type nonrec error = error
    type 'a io = 'a IO.t

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos @@ String.length path - pos

    let key_to_fspath t key = Filename.concat t.dirname key

    let rec create_parent_dir fn perm =
      let parent_dir = Filename.dirname fn in
      match PU.stat parent_dir with
      | exception PU.Unix_error (PU.ENOENT, _, _) ->
        let* () = create_parent_dir parent_dir perm in
        IO.return (PU.mkdir parent_dir perm)
      | _ -> IO.return_unit

    let size t key =
      match PU.openfile (key_to_fspath t key) [PU.O_RDONLY] t.perm with
      | exception Unix.Unix_error (Unix.ENOENT, "open", _) -> IO.return 0
      | fd ->
        Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
        PU.set_nonblock fd;
        IO.return (PU.(fstat fd).st_size)

    let get t key =
      let fd = PU.openfile (key_to_fspath t key) [PU.O_RDONLY] t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      PU.set_nonblock fd;
      let l = PU.(fstat fd).st_size in
      let buf = Bytes.create l in
      let _ = PU.read fd buf 0 l in
      IO.return (Bytes.unsafe_to_string buf)

    let get_partial_values t key ranges = 
      let fd = PU.openfile (key_to_fspath t key) [PU.O_RDONLY] t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      PU.set_nonblock fd;
      let tot = PU.(fstat fd).st_size in
      let l = List.fold_left
        (fun a (s, l) ->
          Option.fold ~none:(Int.max a (tot - s)) ~some:(Int.max a) l) 0 ranges in
      let buf = Bytes.create l in
      IO.return @@ List.fold_right
        (fun (ofs, len) acc ->
          let _ = PU.lseek fd ofs PU.SEEK_SET in
          let size = Option.fold ~none:(tot - ofs) ~some:Fun.id len in
          let _ = PU.read fd buf 0 size in
          Bytes.sub_string buf 0 size :: acc) ranges []

    let set t key v =
      let p = key_to_fspath t key in
      let* () = create_parent_dir p t.perm in
      let fd = PU.openfile p PU.[O_WRONLY; O_TRUNC; O_CREAT] t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      PU.set_nonblock fd;
      ignore (PU.write_substring fd v 0 (String.length v));
      IO.return_unit

    let set_partial_values t key ?(append=false) rvs =
      let flags = match append with
        | false -> PU.[O_WRONLY; O_CREAT] 
        | true -> PU.[O_APPEND; O_WRONLY; O_CREAT] 
      in
      let p = key_to_fspath t key in
      let* () = create_parent_dir p t.perm in
      let fd = PU.openfile p flags t.perm in
      Fun.protect ~finally:(fun () -> PU.close fd) @@ fun () ->
      IO.lift @@ List.fold_left
        (fun acc (ofs, v) ->
          let+ acc in
          if append then ignore (PU.lseek fd 0 PU.SEEK_END)
          else ignore @@ PU.lseek fd ofs PU.SEEK_SET;
          ignore (PU.write_substring fd v 0 (String.length v)); acc) (Ok ()) rvs

    let is_member t key =
      match PU.stat @@ key_to_fspath t key with
      | exception PU.Unix_error (PU.ENOENT, _, _) -> IO.return false
      | _ -> IO.return true

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

    let list t = IO.return (walk t [] (key_to_fspath t ""))
    let list_prefix t prefix = IO.return (walk t [] (key_to_fspath t prefix))
    let rm t key = PU.unlink @@ key_to_fspath t key
    let erase t key = IO.return (rm t key)
    let erase_prefix t pre = IO.bind (list_prefix t pre) @@ fun xs ->
      IO.fold_left (fun acc k -> IO.bind acc (fun () -> erase t k)) IO.return_unit xs

    let list_dir t prefix =
      let dir = key_to_fspath t prefix in
      let h = PU.opendir dir in
      Fun.protect ~finally:(fun () -> PU.closedir h) @@ fun () ->
      IO.return @@ List.partition_map
        (fun x -> match Filename.concat dir x with
         | p when (PU.stat p).st_kind = PU.S_DIR -> Either.right @@ (fspath_to_key t p) ^ "/"
         | p -> Either.left @@ fspath_to_key t p)
        (entries h [])

    let rename t k k' = IO.return (PU.rename (key_to_fspath t k) (key_to_fspath t k'))
  end

  include Zarr.Storage.Make(IO)(Store)

  let create ?(perm=0o700) dirname =
    Zarr.Util.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    Store.{dirname = Zarr.Util.sanitize_dir dirname; perm}
end

let _ =
  Picos_mux_random.run_on ~n_domains:1 @@ fun () ->
  let open Zarr in
  let open Zarr.Codecs in
  let open Zarr.Ndarray in
  let open Zarr.Indexing in
  let open IO.Syntax in

  let store = PicosFSStore.create "picosdata.zarr" in
  let gnode = Result.get_ok (Node.Group.of_path "/some/group") in
  let* () = PicosFSStore.Group.create store gnode in
  let anode = Result.get_ok (Node.Array.(gnode / "name")) in
  let config =
    {chunk_shape = [5; 3; 5]
    ;codecs = [`Bytes LE; `Gzip L5]
    ;index_codecs = [`Bytes BE; `Crc32c]
    ;index_location = Start} in
  let* () = PicosFSStore.Array.create
    ~codecs:[`ShardingIndexed config]
    ~shape:[100; 100; 50]
    ~chunks:[10; 15; 20]
    Char '?' anode store in
  let slice = [R (0, 20); I 10; F] in
  let* x = PicosFSStore.Array.read store anode slice Char in
  let x' = Zarr.Ndarray.map (fun _ -> Random.int 256 |> Char.chr) x in
  let* () = PicosFSStore.Array.write store anode slice x' in
  let* y = PicosFSStore.Array.read store anode slice Char in
  assert (equal x' y);
  IO.return_unit
