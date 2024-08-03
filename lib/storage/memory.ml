module StrMap = Map.Make(struct
  type t = string
  let compare = String.compare
end)

let create () = Atomic.make StrMap.empty

module Impl = struct
  type t = string StrMap.t Atomic.t

  let get t key =
    Option.to_result
      ~none:(`Store_read (Printf.sprintf "%s not found." key)) @@
      StrMap.find_opt key @@ Atomic.get t

  let rec set t key value =
    let m = Atomic.get t in
    let m' = StrMap.add key value m in
    let success = Atomic.compare_and_set t m m' in
    if not success then set t key value

  let list t =
    fst @@ List.split @@ StrMap.bindings @@ Atomic.get t

  let is_member t key =
    StrMap.mem key @@ Atomic.get t

  let rec erase t key =
    let m = Atomic.get t in
    let m' = StrMap.update key (Fun.const None) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then erase t key

  let size t key =
    String.length @@ StrMap.find key @@ Atomic.get t

  let rec erase_prefix t pre =
    let m = Atomic.get t in
    let m' = 
      StrMap.filter_map
        (fun k v ->
          if String.starts_with ~prefix:pre k then None else Some v) m in
    let success = Atomic.compare_and_set t m m' in
    if not success then erase_prefix t pre

  let get_partial_values t key ranges =
    Storage_intf.Base.get_partial_values
      ~get_fn:get t key ranges

  let set_partial_values t key ?(append=false) rv =
    Storage_intf.Base.set_partial_values
      ~set_fn:set ~get_fn:get t key append rv

  let list_prefix t pre =
    Storage_intf.Base.list_prefix ~list_fn:list t pre

  let list_dir t pre =
    Storage_intf.Base.list_dir ~list_fn:list t pre
end
