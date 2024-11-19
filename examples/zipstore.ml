(* This module implements a Zip file zarr store that uses the Eio library for
   non-blocking I/O operations. The main requirement is to implement the signature
   of Zarr.Types.IO. Below we show how to implement this custom Zarr Store.

  To compile & run this example execute the command
    dune exec -- examples/zipstore.exe
  in your shell at the root of this project. *)

module D = Zarr_eio.Storage.Deferred

module ZipStore : sig
  include Zarr.Storage.STORE with module Deferred = D
  val with_open : ?clevel:int -> string -> (t -> 'a) -> 'a
end = struct

  module Z = struct
    module Deferred = D

    type t = {path : string; level : int option}

    let with_open_in path f =
      let ic = Zip.open_in path in
      Fun.protect ~finally:(fun () -> Zip.close_in ic) (fun () -> f ic)

    let with_open_out path f =
      let oc = Zip.open_update path in
      Fun.protect ~finally:(fun () -> Zip.close_out oc) (fun () -> f oc)

    let is_member t key =
      let entry_exists ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> false
        | _ -> true
      in 
      with_open_in t.path (entry_exists ~key)

    let size t key =
      let entry_size ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> 0
        | e -> e.uncompressed_size
      in
      with_open_in t.path (entry_size ~key)

    let get t key =
      let read_entry ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> raise (Zarr.Storage.Key_not_found key)
        | e -> Zip.read_entry ic e
      in
      with_open_in t.path (read_entry ~key)

    let get_partial_values t key ranges =
      let read_range ~data ~size (ofs, len) = match len with
        | Some l -> String.sub data ofs l
        | None -> String.sub data ofs (size - ofs)
      in
      let data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let list t =
      let entry_filename = function
        | (e : Zip.entry) when not e.is_directory -> Some e.filename
        | _ -> None
      in
      let entries = with_open_in t.path (fun ic -> Zip.entries ic) in
      List.filter_map entry_filename entries

    let list_dir t prefix =
      let module S = Set.Make(String) in
      let n = String.length prefix in
      let add_entry_with_prefix ((l, r) as acc) = function
        | (e : Zip.entry) when e.is_directory -> acc
        | e when not (String.starts_with ~prefix e.filename) -> acc
        | e when String.contains_from e.filename n '/' ->
          let key = e.filename in
          let pre = String.sub key 0 (1 + String.index_from key n '/') in
          S.add pre l, r
        | e -> l, e.filename :: r
      in
      let entries = with_open_in t.path (fun ic -> Zip.entries ic) in
      let prefs, keys = List.fold_left add_entry_with_prefix (S.empty, []) entries in
      keys, S.elements prefs

    let set t key value =
      with_open_out t.path (fun oc -> Zip.add_entry ?level:t.level value oc key)

    let set_partial_values t key ?(append=false) rvs =
      let ov = try get t key with
        | Zarr.Storage.Key_not_found _ -> String.empty
      in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      set t key (List.fold_left f ov rvs)

    let add_to_zip ~oc ~level (path, v) = Zip.add_entry ?level v oc path

    let rename t prefix new_prefix =
      let add_pair ~ic ~prefix ~new_prefix acc = function
        | (e : Zip.entry) when not (String.starts_with ~prefix e.filename) ->
          (e.filename, Zip.read_entry ic e) :: acc
        | e ->
          let l = String.length prefix in
          let path = new_prefix ^ String.sub e.filename l (String.length e.filename - l) in
          (path, Zip.read_entry ic e) :: acc
      in
      let rename_entries ic =
        List.fold_left (add_pair ~ic ~prefix ~new_prefix) [] (Zip.entries ic)
      in
      let pairs = with_open_in t.path rename_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path @@ fun oc -> List.iter (add_to_zip ~oc ~level:t.level) pairs

    let erase t key =
      let filter ~ic acc = function
        | (e : Zip.entry) when e.filename = key -> acc
        | e -> (e.filename, Zip.read_entry ic e) :: acc
      in
      let filter_entries ic = List.fold_left (filter ~ic) [] (Zip.entries ic) in
      let pairs = with_open_in t.path filter_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path @@ fun oc -> List.iter (add_to_zip ~oc ~level:t.level) pairs

    let erase_prefix t prefix =
      let filter ~ic ~prefix acc = function
        | (e : Zip.entry) when String.starts_with ~prefix e.filename -> acc
        | e -> (e.filename, Zip.read_entry ic e) :: acc
      in
      let filter_entries ic = List.fold_left (filter ~ic ~prefix) [] (Zip.entries ic) in
      let pairs = with_open_in t.path filter_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path @@ fun oc -> List.iter (add_to_zip ~oc ~level:t.level) pairs
  end

  include Zarr.Storage.Make(Z)

  let with_open ?clevel path f =
    if not @@ Sys.file_exists path then begin
      Zip.(close_out @@ open_out path)
    end;
    let level = match clevel with 
      | Some l when l < 0 || l > 9 ->
        raise @@ invalid_arg (Printf.sprintf "wrong compression level: %d" l)
      | l -> l
    in
    f Z.{path; level}
end

let _ =
  Eio_main.run @@ fun _ ->
  let open Zarr in
  let open Zarr.Ndarray in
  let open Zarr.Indexing in

  let test_functionality store = 
    let xs, _ = ZipStore.hierarchy store in
    let anode = List.hd @@ List.filter
      (fun node -> Node.Array.to_path node = "/some/group/name") xs in
    let slice = [|R [|0; 20|]; I 10; R [||]|] in
    let x = ZipStore.Array.read store anode slice Char in
    let x' = Zarr.Ndarray.map (fun _ -> Random.int 256 |> Char.chr) x in
    ZipStore.Array.write store anode slice x';
    let y = ZipStore.Array.read store anode slice Char in
    assert (Zarr.Ndarray.equal x' y);
    ZipStore.Array.rename store anode "name2";
    let exists = ZipStore.Array.exists store @@ Node.Array.of_path "/some/group/name2" in
    assert exists;
    ZipStore.clear store  (* deletes all zip entries *)
  in
  ZipStore.with_open "examples/data/testdata.zip" test_functionality;
  print_endline "Zip store has been updated."
