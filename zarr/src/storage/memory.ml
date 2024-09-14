module StrMap = Map.Make(struct
  type t = string
  let compare = String.compare
end)

let create () = Atomic.make StrMap.empty

module Make (Deferred : Types.Deferred) = struct
  module Deferred = Deferred
  open Deferred.Syntax

  type t = string StrMap.t Atomic.t

  let get t key =
    match StrMap.find_opt key @@ Atomic.get t with
    | Some v -> Deferred.return v
    | None -> raise @@ Storage.Key_not_found key

  let rec set t key value =
    let m = Atomic.get t in
    let m' = StrMap.add key value m in
    let success = Atomic.compare_and_set t m m' in
    if not success then set t key value else Deferred.return_unit

  let list t =
    Deferred.return @@ fst @@ List.split @@ StrMap.bindings @@ Atomic.get t

  let is_member t key =
    Deferred.return @@ StrMap.mem key @@ Atomic.get t

  let rec erase t key =
    let m = Atomic.get t in
    let m' = StrMap.update key (Fun.const None) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then erase t key else Deferred.return_unit

  let size t key =
    Deferred.return @@ String.length @@ StrMap.find key @@ Atomic.get t

  let rec erase_prefix t pre =
    let m = Atomic.get t in
    let m' = 
      StrMap.filter_map
        (fun k v ->
          if String.starts_with ~prefix:pre k then None else Some v) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then erase_prefix t pre else Deferred.return_unit

  let get_partial_values t key ranges =
    let+ v = get t key in
    let size = String.length v in
    List.fold_left
      (fun acc (ofs, len) ->
        let f x = String.sub v ofs x :: acc in
        Option.fold ~none:(f (size - ofs)) ~some:f len) [] @@ List.rev ranges

  let rec set_partial_values t key ?(append=false) rv =
    let f = if append then fun acc (_, v) -> acc ^ v else
      fun acc (rs, v) ->
        let s = Bytes.of_string acc in
        String.(length v |> Bytes.blit_string v 0 s rs);
        Bytes.to_string s in
    let m = Atomic.get t in
    let ov = StrMap.find key m in
    let m' = StrMap.add key (List.fold_left f ov rv) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then set_partial_values t key ~append rv
    else Deferred.return_unit

  let list_dir t prefix =
    let m = Atomic.get t in
    let module S = Set.Make(String) in
    let n = String.length prefix in
    let prefs, keys =
      StrMap.fold
        (fun key _ ((l, r) as a) ->
          let pred = String.starts_with ~prefix key in
          match key with
          | k when pred && String.contains_from k n '/' ->
            let l' = S.add String.(sub k 0 @@ 1 + index_from k n '/') l in l', r
          | k when pred -> l, k :: r
          | _ -> a) m (S.empty, [])
    in Deferred.return (keys, S.elements prefs)

  let rec rename t ok nk =
    let m = Atomic.get t in
    let m1, m2 = StrMap.partition (fun k _ -> String.starts_with ~prefix:ok k) m in
    let l = String.length ok in
    let s = Seq.map
      (fun (k, v) -> nk ^ String.(length k - l |> sub k l), v) @@ StrMap.to_seq m1 in
    let m' = StrMap.add_seq s m2 in
    if Atomic.compare_and_set t m m' then Deferred.return_unit else rename t ok nk
end
