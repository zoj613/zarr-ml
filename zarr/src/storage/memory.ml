module M = Map.Make(String)

let create () : string M.t Atomic.t = Atomic.make M.empty

module Make (Deferred : Types.Deferred) = struct
  module Deferred = Deferred
  open Deferred.Syntax

  type t = string M.t Atomic.t

  let get t key =
    match M.find_opt key @@ Atomic.get (t : t) with
    | Some v -> Deferred.return v
    | None -> raise @@ Storage.Key_not_found key

  let rec set t key value =
    let m = Atomic.get (t : t) in
    if Atomic.compare_and_set t m (M.add key value m)
    then Deferred.return_unit else set t key value 

  let list t =
    Deferred.return @@ M.fold (fun k _ acc -> k :: acc) (Atomic.get t) []

  let is_member t key =
    Deferred.return @@ M.mem key (Atomic.get t)

  let rec erase t key =
    let m = Atomic.get t in
    if Atomic.compare_and_set t m @@ M.update key (Fun.const None) m
    then Deferred.return_unit else erase t key

  let size t key =
    Deferred.return @@ String.length @@ M.find key @@ Atomic.get t

  let rec erase_prefix t prefix =
    let pred ~prefix k v = if String.starts_with ~prefix k then None else Some v in
    let m = Atomic.get t in
    if Atomic.compare_and_set t m @@ M.filter_map (pred ~prefix) m
    then Deferred.return_unit else erase_prefix t prefix

  let get_partial_values t key ranges =
    let add ~value ~size acc (ofs, len) = match len with
      | Some l -> String.sub value ofs l :: acc
      | None -> String.sub value ofs (size - ofs) :: acc
    in
    let+ value = get t key in
    let size = String.length value in
    List.fold_left (add ~value ~size) [] (List.rev ranges)

  let rec set_partial_values t key ?(append=false) rv =
    let f = if append then fun acc (_, v) -> acc ^ v else
      fun acc (rs, v) ->
        let s = Bytes.of_string acc in
        String.(length v |> Bytes.blit_string v 0 s rs);
        Bytes.to_string s in
    let m = Atomic.get (t : t) in
    let ov = M.find key m in
    let m' = M.add key (List.fold_left f ov rv) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then set_partial_values t key ~append rv
    else Deferred.return_unit

  let list_dir t prefix =
    let module S = Set.Make(String) in
    let m = Atomic.get (t : t) in
    let add ~size ~prefix key _ ((l, r) as a) =
      let pred = String.starts_with ~prefix key in
      match key with
      | k when pred && String.contains_from k size '/' ->
        S.add String.(sub k 0 @@ 1 + index_from k size '/') l, r
      | k when pred -> l, k :: r
      | _ -> a
    in
    let size = String.length prefix in
    let prefixes, keys = M.fold (add ~prefix ~size) m (S.empty, []) in
    Deferred.return (keys, S.elements prefixes)

  let rec rename t prefix new_prefix =
    let add ~prefix ~new_prefix k v acc =
      if not (String.starts_with ~prefix k) then M.add k v acc else
      let l = String.length prefix in
      let k' = new_prefix ^ String.sub k l (String.length k - l) in
      M.add k' v acc
    in
    let m = Atomic.get (t : t) in
    let m' = M.fold (add ~prefix ~new_prefix) m M.empty in
    if Atomic.compare_and_set t m m'
    then Deferred.return_unit else rename t prefix new_prefix
end
