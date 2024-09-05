(* This module implements a Zip file zarr store that is Lwt-aware.
   It supports both read and write operations. This is because the
   underlying Zip library used reads all Zip file bytes into memory. All
   store updates are done in-memory and thus to update the actual zip file
   we must persist the data using `MemoryZipStore.write_to_file`.

   The main requirement is to implement the signature of Zarr.Types.IO.
   We use Zarr_lwt Deferred module for `Deferred` so that the store can be
   Lwt-aware.

  To compile & run this example execute the command
    dune exec -- examples/inmemory_zipstore.exe
  in your shell at the root of this project. *) 

module MemoryZipStore : sig
  include Zarr.Storage.STORE with type 'a Deferred.t = 'a Lwt.t
  (*val create : ?level:Zipc_deflate.level -> string -> t *)
  val with_open : ?level:Zipc_deflate.level -> string -> (t -> 'a Deferred.t) -> 'a Deferred.t
end = struct
  module M = Map.Make(String)

  module Z = struct
    module Deferred = Zarr_lwt.Deferred
    open Deferred.Infix
    open Deferred.Syntax

    type t =
      {mutable ic : Zipc.t
      ;mutex : Lwt_mutex.t
      ;level : Zipc_deflate.level
      ;path : string}

    let is_member t key =
      Deferred.return @@ Zipc.mem key t.ic

    let size t key =
      Deferred.return @@
      match Zipc.find key t.ic with
      | None -> 0
      | Some m ->
        match Zipc.Member.kind m with
        | Zipc.Member.Dir -> 0
        | Zipc.Member.File f -> Zipc.File.decompressed_size f

    let get t key =
      Deferred.return @@
      match Zipc.find key t.ic with
      | None -> raise (Zarr.Storage.Key_not_found key)
      | Some m ->
        match Zipc.Member.kind m with
        | Zipc.Member.Dir -> failwith "cannot get size of directory." 
        | Zipc.Member.File f ->
          match Zipc.File.to_binary_string f with
          | Error e -> failwith e
          | Ok s -> s

    let get_partial_values t key ranges =
      let+ data = get t key in
      let size = String.length data in
      ranges |> List.map @@ fun (ofs, len) ->
      let f v = String.sub data ofs v in
      Option.fold ~none:(f (size - ofs)) ~some:f len

    let list t =
      Deferred.return @@ Zipc.fold
        (fun m acc ->
          match Zipc.Member.kind m with
          | Zipc.Member.Dir -> acc
          | Zipc.Member.File _ -> Zipc.Member.path m :: acc) t.ic []

    let list_dir t prefix =
      let module S = Set.Make(String) in
      let n = String.length prefix in
      let m = Zipc.to_string_map t.ic in
      let prefs, keys =
        M.fold
          (fun key v ((l, r) as acc) ->
            match Zipc.Member.kind v with
            | Zipc.Member.Dir -> acc
            | Zipc.Member.File _ ->
              let pred = String.starts_with ~prefix key in
              match key with
              | k when pred && String.contains_from k n '/' ->
                S.add String.(sub k 0 @@ 1 + index_from k n '/') l, r
              | k when pred -> l, k :: r
              | _ -> acc) m (S.empty, [])
      in Deferred.return (keys, S.elements prefs)

    let set t key value =
      match Zipc.File.deflate_of_binary_string ~level:t.level value with
      | Error e -> failwith e
      | Ok f ->
        match Zipc.Member.(make ~path:key @@ File f) with
        | Error e -> failwith e
        | Ok m ->
          Lwt_mutex.with_lock
            t.mutex
            (fun () ->
              t.ic <- Zipc.add m t.ic;
              Deferred.return_unit)

    let set_partial_values t key ?(append=false) rv =
      let f = 
        if append then
          fun acc (_, v) ->
            Deferred.return @@ acc ^ v
        else
          fun acc (rs, v) ->
            let s = Bytes.of_string acc in
            String.(length v |> Bytes.blit_string v 0 s rs);
            Deferred.return @@ Bytes.to_string s
      in
      match Zipc.Member.kind (Option.get @@ Zipc.find key t.ic) with
      | Zipc.Member.Dir -> Deferred.return_unit 
      | Zipc.Member.File file ->
        match Zipc.File.to_binary_string file with
        | Error e -> failwith e
        | Ok s -> Deferred.fold_left f s rv >>= set t key

    let erase t key =
      Lwt_mutex.with_lock
        t.mutex
        (fun () ->
          t.ic <- Zipc.remove key t.ic;
          Deferred.return_unit)

    let erase_prefix t prefix =
      let m = Zipc.to_string_map t.ic in
      let m' =
        M.filter_map
          (fun k v ->
            if String.starts_with ~prefix k then None else Some v) m in
      Lwt_mutex.with_lock
        t.mutex
        (fun () ->
          t.ic <- Zipc.of_string_map m';
          Deferred.return_unit)
  end

  (* this functor generates the public signature of our Zip file store. *)
  include Zarr.Storage.Make(Z)

  (* now we create functions to open and close the store. *)

  (*let create ?(level=`Default) path = Z.{ic = Zipc.empty; level; path} *)

  let with_open ?(level=`Default) path f =
    let s = In_channel.(with_open_bin path input_all) in
    let t = match Zipc.of_binary_string s with
      | Ok ic -> Z.{ic; level; path; mutex = Lwt_mutex.create ()}
      | Error e -> failwith e
    in
    Lwt.finalize
      (fun () -> f t)
      (fun () ->
        Lwt_io.with_file
          ~flags:Unix.[O_WRONLY; O_TRUNC; O_CREAT; O_NONBLOCK]
          ~mode:Lwt_io.Output
          t.path
          (fun oc ->
            let open Lwt.Syntax in
            match Zipc.to_binary_string t.ic with
            | Error e -> failwith e
            | Ok s' ->
              if String.equal s s' then Lwt.return_unit else
              let* () = Lwt_io.write oc s' in Lwt_io.flush oc))
end

let _ =
  Lwt_main.run @@ begin
  let open Zarr.Node in
  let open MemoryZipStore.Deferred.Syntax in

  let printlist = [%show: string list] in
  MemoryZipStore.with_open "examples/data/testdata.zip" @@ fun store ->
  let* xs, _ = MemoryZipStore.find_all_nodes store in
  print_endline @@ "All array nodes: " ^ printlist (List.map ArrayNode.to_path xs);
  let anode = List.hd @@ List.filter
    (fun node -> ArrayNode.to_path node = "/some/group/name") xs in
  let slice = Owl_types.[|R [0; 20]; I 10; R []|] in
  let* x = MemoryZipStore.read_array store anode slice Zarr.Ndarray.Char in
  let x' = x |> Zarr.Ndarray.map @@ fun _ -> Random.int 256 |> Char.chr in
  let* () = MemoryZipStore.write_array store anode slice x' in
  let+ y = MemoryZipStore.read_array store anode slice Zarr.Ndarray.Char in
  assert (Zarr.Ndarray.equal x' y)
  end;
  print_endline "Zip store has been update."
