(* This module implements a Zip file zarr store that is Lwt-aware.
   It supports both read and write operations. This is because the
   underlying Zip library used reads all Zip file bytes into memory. All
   store updates are done in-memory and thus to update the actual zip file
   we must write the update bytes to disk. The `with_open` convenience
   function serves this purpose; it ensures that any updates to the store
   are written to the zip file upon exit.

   The main requirement is to implement the signature of Zarr.Types.IO.
   We use Zarr_lwt's Deferred module for `Deferred` so that the store can be
   Lwt-aware.

  To compile & run this example execute the command
    dune exec -- examples/inmemory_zipstore.exe
  in your shell at the root of this project. *) 

module ZipStore : sig
  include Zarr.Storage.STORE with module Deferred = Zarr_lwt.Deferred
  val with_open : ?level:Zipc_deflate.level -> Unix.file_perm -> string -> (t -> 'a Deferred.t) -> 'a Deferred.t
end = struct
  module M = Map.Make(String)

  module Z = struct
    module Deferred = Zarr_lwt.Deferred
    open Deferred.Syntax

    type t = {ic : Zipc.t Atomic.t; level : Zipc_deflate.level}

    let is_member t key =
      Deferred.return @@ Zipc.mem key @@ Atomic.get t.ic

    let size t key =
      Deferred.return @@ 
      match Zipc.find key @@ Atomic.get t.ic with
      | None -> 0
      | Some m ->
        match Zipc.Member.kind m with
        | Zipc.Member.Dir -> 0
        | Zipc.Member.File f -> Zipc.File.decompressed_size f

    let get t key =
      Deferred.return @@
      match Zipc.find key @@ Atomic.get t.ic with
      | None -> raise (Zarr.Storage.Key_not_found key)
      | Some m ->
        match Zipc.Member.kind m with
        | Zipc.Member.Dir -> failwith "A chunk key cannot be a directory." 
        | Zipc.Member.File f ->
          Result.fold ~error:failwith ~ok:Fun.id @@ Zipc.File.to_binary_string f

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
          | Zipc.Member.File _ -> Zipc.Member.path m :: acc) (Atomic.get t.ic) []

    let list_dir t prefix =
      let module S = Set.Make(String) in
      let n = String.length prefix in
      let m = Zipc.to_string_map @@ Atomic.get t.ic in
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

    let rec set t key value =
      match Zipc.File.deflate_of_binary_string ~level:t.level value with
      | Error e -> failwith e
      | Ok f ->
        match Zipc.Member.(make ~path:key @@ File f) with
        | Error e -> failwith e
        | Ok m ->
          let z = Atomic.get t.ic in
          if Atomic.compare_and_set t.ic z @@ Zipc.add m z
          then Deferred.return_unit else set t key value

    let rec set_partial_values t key ?(append=false) rv =
      let z = Atomic.get t.ic in
      let mem = match Zipc.find key z with
        | Some m -> m
        | None ->
          let empty = Result.fold
            ~error:failwith ~ok:Fun.id @@ Zipc.File.stored_of_binary_string String.empty in
          Result.fold
            ~error:failwith ~ok:Fun.id @@ Zipc.Member.make ~path:key (Zipc.Member.File empty)
      in
      match Zipc.Member.kind mem with
      | Zipc.Member.Dir -> Deferred.return_unit 
      | Zipc.Member.File file ->
        match Zipc.File.to_binary_string file with
        | Error e -> failwith e
        | Ok s ->
          let f = if append || s = String.empty then
            fun acc (_, v) -> Deferred.return @@ acc ^ v else
            fun acc (rs, v) ->
              let s = Bytes.unsafe_of_string acc in
              String.(length v |> Bytes.blit_string v 0 s rs);
              Deferred.return @@ Bytes.unsafe_to_string s
          in
          let* value = Deferred.fold_left f s rv in
          match Zipc.File.deflate_of_binary_string ~level:t.level value with
          | Error e -> failwith e
          | Ok f ->
            match Zipc.Member.(make ~path:key @@ File f) with
            | Error e -> failwith e
            | Ok m ->
              if Atomic.compare_and_set t.ic z @@ Zipc.add m z
              then Deferred.return_unit else set_partial_values t key ~append rv

    let rec erase t key =
      let z = Atomic.get t.ic in
      let z' = Zipc.remove key z in
      if Atomic.compare_and_set t.ic z z'
      then Deferred.return_unit else erase t key

    let rec erase_prefix t prefix =
      let z = Atomic.get t.ic in
      let m = Zipc.to_string_map z in
      let m' = M.filter_map
        (fun k v -> if String.starts_with ~prefix k then None else Some v) m in
      let z' = Zipc.of_string_map m' in
      if Atomic.compare_and_set t.ic z z'
      then Deferred.return_unit else erase_prefix t prefix

    (* Adapted from: https://github.com/dbuenzli/zipc/issues/8#issuecomment-2392417890 *)
    let rec rename t prefix new_prefix =
      let rename_member ~prefix ~new_prefix m =
        let path = Zipc.Member.path m in
        if not (String.starts_with ~prefix path) then m else
        let l = String.length prefix in
        let path = new_prefix ^ String.sub path l (String.length path - l) in
        let mtime = Zipc.Member.mtime m in
        let mode = Zipc.Member.mode m in
        let kind = Zipc.Member.kind m in
        match Zipc.Member.make ~mtime ~mode ~path kind with
        | Ok m' -> m' | Error e -> failwith e
      in
      let z = Atomic.get t.ic in
      let add m acc = Zipc.add (rename_member ~prefix ~new_prefix m) acc in
      let z' = Zipc.fold add z Zipc.empty in
      if Atomic.compare_and_set t.ic z z'
      then Deferred.return_unit else rename t prefix new_prefix
  end
  (* this functor generates the public signature of our Zip file store. *)
  include Zarr.Storage.Make(Z)

  let with_open ?(level=`Default) perm path f =
    let s = In_channel.(with_open_bin path input_all) in
    let x = match Zipc.of_binary_string s with
      | Ok z -> Z.{ic = Atomic.make z; level}
      | Error e -> failwith e
    in
    let open Deferred.Syntax in
    let+ out = f x in
    let flags = [Open_wronly; Open_trunc; Open_creat] in
    match Zipc.to_binary_string @@ Atomic.get x.ic with
    | Error e -> failwith e
    | Ok v ->
      Out_channel.with_open_gen flags perm path @@ fun oc ->
      Out_channel.output_string oc v;
      Out_channel.flush oc;
      out
end

let _ =
  Lwt_main.run @@ begin
  let open Zarr in
  let open Zarr.Ndarray in
  let open Zarr.Indexing in
  let open ZipStore.Deferred.Syntax in

  ZipStore.with_open 0o700 "examples/data/testdata.zip" @@ fun store ->
  let* xs, _ = ZipStore.hierarchy store in
  let anode = List.hd @@ List.filter
    (fun node -> Node.Array.to_path node = "/some/group/name") xs in
  let slice = [|R [|0; 20|]; I 10; R [||]|] in
  let* x = ZipStore.Array.read store anode slice Char in
  let x' = x |> Zarr.Ndarray.map @@ fun _ -> Random.int 256 |> Char.chr in
  let* () = ZipStore.Array.write store anode slice x' in
  let* y = ZipStore.Array.read store anode slice Char in
  assert (Zarr.Ndarray.equal x' y);
  let* () = ZipStore.Array.rename store anode "name2" in
  let+ exists = ZipStore.Array.exists store @@ Node.Array.of_path "/some/group/name2" in
  assert exists
  end;
  print_endline "Zip store has been updated."
