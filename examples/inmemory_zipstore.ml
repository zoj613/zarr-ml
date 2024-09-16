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
  include Zarr.Storage.STORE with type 'a Deferred.t = 'a Lwt.t
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
          Lwt_mutex.with_lock t.mutex @@ fun () ->
          Deferred.return (t.ic <- Zipc.add m t.ic)

    let set_partial_values t key ?(append=false) rv =
      let f = 
        if append then
          fun acc (_, v) ->
            Deferred.return @@ acc ^ v
        else
          fun acc (rs, v) ->
            let s = Bytes.unsafe_of_string acc in
            String.(length v |> Bytes.blit_string v 0 s rs);
            Deferred.return @@ Bytes.unsafe_to_string s
      in
      match Zipc.Member.kind (Option.get @@ Zipc.find key t.ic) with
      | Zipc.Member.Dir -> Deferred.return_unit 
      | Zipc.Member.File file ->
        match Zipc.File.to_binary_string file with
        | Error e -> failwith e
        | Ok s -> Deferred.fold_left f s rv >>= set t key

    let erase t key =
      Lwt_mutex.with_lock t.mutex @@ fun () ->
      Deferred.return (t.ic <- Zipc.remove key t.ic)

    let erase_prefix t prefix =
      let m = Zipc.to_string_map t.ic in
      let m' = M.filter_map
        (fun k v -> if String.starts_with ~prefix k then None else Some v) m in
      Lwt_mutex.with_lock t.mutex @@ fun () ->
      Deferred.return (t.ic <- Zipc.of_string_map m')

    let rename t ok nk =
      Lwt_mutex.with_lock t.mutex @@ fun () ->
      let m = Zipc.to_string_map t.ic in
      let m1, m2 = M.partition (fun k _ -> String.starts_with ~prefix:ok k) m in
      let l = String.length ok in
      let s = Seq.map
        (fun (k, v) -> nk ^ String.(length k - l |> sub k l), v) @@ M.to_seq m1 in
      t.ic <- Zipc.of_string_map @@ M.add_seq s m2; Lwt.return_unit
  end
  (* this functor generates the public signature of our Zip file store. *)
  include Zarr.Storage.Make(Z)

  let with_open ?(level=`Default) path f =
    let s = In_channel.(with_open_bin path input_all) in
    let t = match Zipc.of_binary_string s with
      | Ok ic -> Z.{ic; level; path; mutex = Lwt_mutex.create ()}
      | Error e -> failwith e
    in
    Lwt.finalize (fun () -> f t) @@ fun () ->
    let flags = Unix.[O_WRONLY; O_TRUNC; O_CREAT; O_NONBLOCK] in
    Lwt_io.with_file ~flags ~mode:Lwt_io.Output t.path @@ fun oc ->
    Result.fold ~error:failwith ~ok:(Lwt_io.write oc) @@ Zipc.to_binary_string t.ic
end

let _ =
  Lwt_main.run @@ begin
  let open Zarr in
  let open Zarr.Ndarray in
  let open Zarr.Indexing in
  let open ZipStore.Deferred.Syntax in

  ZipStore.with_open "examples/data/testdata.zip" @@ fun store ->
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
