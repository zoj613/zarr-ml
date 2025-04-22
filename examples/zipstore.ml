(* This module implements a Zip archive zarr store that uses the Eio library for
   non-blocking I/O operations. The main requirement is to implement the signature
   of Zarr.Types.Store. Below we show how to implement this custom Zarr Store.

  To compile & run this example execute the command
    dune exec -- examples/zipstore.exe
  in your shell at the root of this project. *)

module IO = Zarr_eio.Storage.IO

type error = [ `Read of string | `Write of string ]

module ZipStore : sig
  include Zarr.Storage.S with type 'a io := 'a and type error = error
  val with_open :
    ?level:[ `None | `Fast | `Default | `Best ] ->
    ?perm:int ->
    [< `Read_only | `Read_write ] ->
    string ->
    (t -> ('a, [> `Zarr of error ] as 'b) result IO.t) ->
    ('a, 'b) result IO.t 
  (** [with_open mode p f] opens the zip archive at path [p] and applies
      function [f] to its open handle and writes any changes back to the zip
      archive if [mode] is [`Read_write], otherwise discards them at exit.
      If [p] does not exist, a handle to an empty zip archive is opened.
      Note that this function loads the entire zip archive bytes into memory,
      so care must be taken to ensure that these bytes can fit into the local
      machine's available memory. For now it does not handle ZIP64. ZIP64 is
      needed if your ZIP archive or decompressed file sizes exceed 2{^32}-1
      bytes or if you need more than 65535 archive members.

      {ul 
      {- [level] is the DEFLATE algorithm compression level used when writing
        data to the store and defaults to [`Default]. Choose [`None] for no
        compression, [`Fast] for best speed, [`Best] for high compression rate
        and [`Default] for a mix of good speed and compression rate.}
      {- [perm] is the file permission to use when opening an existing zip file
        and defaults to [0o700].}
      } *)
end = struct
  open IO.Syntax

  module Store = struct
    type t = {atomic_ref : Zipc.t Atomic.t; level : Zipc_deflate.level}
    type nonrec error = error
    type 'a io = 'a IO.t

    let is_member t key = IO.return (Zipc.mem key @@ Atomic.get t.atomic_ref)

    let size t key = match Zipc.find key (Atomic.get t.atomic_ref) with
      | None -> IO.return 0
      | Some m -> match Zipc.Member.kind m with
        | Zipc.Member.Dir -> IO.return 0
        | Zipc.Member.File f -> IO.return (Zipc.File.decompressed_size f)

    let get t key = match Zipc.find key (Atomic.get t.atomic_ref) with
      | None -> IO.error (`Zarr (`Read (Printf.sprintf "%s not found" key)))
      | Some m -> match Zipc.Member.kind m with
        | Zipc.Member.Dir -> IO.return String.empty
        | Zipc.Member.File f -> match Zipc.File.to_binary_string f with
          | Error e -> IO.error (`Zarr (`Read e))
          | Ok _ as r -> IO.lift r

    let get_partial_values t key ranges =
      let read_range ~data ~size (ofs, len) = match len with
        | None -> String.sub data ofs (size - ofs)
        | Some l -> String.sub data ofs l
      in
      let+ data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let list t =
      let zip = Atomic.get t.atomic_ref in
      IO.return (Zipc.fold (fun mem acc -> Zipc.Member.path mem :: acc) zip [])

    let list_dir t prefix =
      let module S = Set.Make(String) in
      let accumulate ~prefix m ((l, r) as acc) =
        let key = Zipc.Member.path m in
        if not (String.starts_with ~prefix key) then acc else
        let n = String.length prefix in
        if not (String.contains_from key n '/') then key :: l, r else
        l, S.add StringLabels.(sub ~pos:0 ~len:(1 + index_from key n '/') key) r
      in
      let zip = Atomic.get t.atomic_ref in 
      let ks, ps = Zipc.fold (accumulate ~prefix) zip ([], S.empty) in
      IO.return (ks, S.elements ps)

    let rec set t key value =
      match Zipc.File.deflate_of_binary_string ~level:t.level value with
      | Error e -> Error (`Zarr (`Write e))
      | Ok file -> match Zipc.Member.(make ~path:key (File file)) with
        | Error e -> Error (`Zarr (`Write e))
        | Ok m ->
          let zip = Atomic.get t.atomic_ref in
          if Atomic.compare_and_set t.atomic_ref zip (Zipc.add m zip)
          then IO.return_unit else set t key value

    let rec set_partial_values t key ?(append=false) rv =
      let z = Atomic.get t.atomic_ref in
      let* mem = match Zipc.find key z with
        | Some m -> IO.return m
        | None ->
          let empty_file = Zipc.File.deflate_of_binary_string ~level:t.level String.empty in
          match Zipc.Member.(make ~path:key @@ File (Result.get_ok empty_file)) with
          | Error e -> IO.error (`Zarr (`Write e))
          | Ok _ as r -> IO.lift r 
      in
      let* ov = match Zipc.Member.kind mem with
        | Zipc.Member.Dir -> IO.return String.empty
        | Zipc.Member.File f -> match Zipc.File.to_binary_string f with
          | Error e -> IO.error (`Zarr (`Write e))
          | Ok _ as r ->  IO.lift r
      in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      let ov' = List.fold_left f ov rv in
      match Zipc.File.deflate_of_binary_string ~level:t.level ov' with
      | Error e -> Error (`Zarr (`Write e))
      | Ok file -> match Zipc.Member.(make ~path:key (File file)) with
        | Error e -> Error (`Zarr (`Write e))
        | Ok m ->
          if Atomic.compare_and_set t.atomic_ref z (Zipc.add m z)
          then IO.return_unit else set_partial_values t key ~append rv

    let rec erase t key =
      let zip = Atomic.get t.atomic_ref in
      if Atomic.compare_and_set t.atomic_ref zip (Zipc.remove key zip)
      then IO.return_unit else erase t key

    let rec erase_prefix t prefix =
      let accumulate ~prefix m acc =
        if String.starts_with ~prefix (Zipc.Member.path m) then acc else Zipc.add m acc
      in
      let z = Atomic.get t.atomic_ref in
      let z' = Zipc.fold (accumulate ~prefix) z Zipc.empty in
      if Atomic.compare_and_set t.atomic_ref z z'
      then IO.return_unit else erase_prefix t prefix

    (* Adapted from: https://github.com/dbuenzli/zipc/issues/8#issuecomment-2392417890 *)
    let rec rename t prefix new_prefix =
      let accumulate ~prefix ~new_prefix m acc =
        let path = Zipc.Member.path m in
        if not (String.starts_with ~prefix path) then Result.map (Zipc.add m) acc else
        let l = String.length prefix in
        let path = new_prefix ^ String.sub path l (String.length path - l) in
        let mtime = Zipc.Member.mtime m in
        let mode = Zipc.Member.mode m in
        let kind = Zipc.Member.kind m in
        match Zipc.Member.make ~mtime ~mode ~path kind with
        | Error _ as e -> e 
        | Ok m' -> Result.map (Zipc.add m') acc 
      in
      let z = Atomic.get t.atomic_ref in
      match Zipc.fold (accumulate ~prefix ~new_prefix) z (Ok Zipc.empty) with
      | Error e -> IO.error (`Zarr (`Write e))
      | Ok z' -> match Atomic.compare_and_set t.atomic_ref z z' with
        | true -> IO.return_unit
        | false -> rename t prefix new_prefix
  end

  include Zarr.Storage.Make(IO)(Store)

  let with_open ?(level=`Default) ?(perm=0o700) mode path f =
    let make z = Store.{atomic_ref = Atomic.make z; level} in
    let* x = match Sys.file_exists path with
      | false -> IO.return (make Zipc.empty)
      | true -> match Zipc.of_binary_string In_channel.(with_open_bin path input_all) with
        | Ok z -> IO.return (make z)
        | Error e -> IO.error (`Zarr (`Read e))
    in
    match mode with
    | `Read_only -> f x
    | `Read_write ->
      let* out = f x in
      match Zipc.to_binary_string (Atomic.get x.atomic_ref) with
      | Error e -> IO.error (`Zarr (`Write e))
      | Ok s ->
        let flags = [Open_wronly; Open_trunc; Open_creat] in
        Out_channel.with_open_gen flags perm path @@ fun oc ->
        Out_channel.output_string oc s;
        flush oc;
        IO.return out
end

let _ =
  Eio_main.run @@ fun _ ->
  let open Zarr in
  let open Zarr.Ndarray in
  let open Zarr.Indexing in
  let open IO.Syntax in

  let test_functionality store = 
    let* xs, _ = ZipStore.hierarchy store in
    let anode = List.hd @@ List.filter
      (fun node -> String.equal (Node.Array.to_path node) "/some/group/name") xs in
    let slice = [R (0, 20); I 10; F] in
    let* x = ZipStore.Array.read store anode slice Char in
    let x' = Zarr.Ndarray.map (fun _ -> Random.int 256 |> Char.chr) x in
    let* () = ZipStore.Array.write store anode slice x' in
    let* y = ZipStore.Array.read store anode slice Char in
    assert (Zarr.Ndarray.equal x' y);
    let* anode' = ZipStore.Array.rename store anode "name2" in
    let* exists = ZipStore.Array.exists store anode' in
    assert exists;
    ZipStore.clear store  (* deletes all zip entries *)
  in
  ZipStore.with_open `Read_only "examples/data/testdata.zip" test_functionality
