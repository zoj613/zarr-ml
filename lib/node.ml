type error =
  [ `Node_invariant of string ]

(* Check if the path's name satisfies path invariants *)
let rep_ok name =
  (String.empty <> name) &&
  not (String.contains name '/') &&
  not (String.for_all (Char.equal '.') name) &&
  not (String.starts_with ~prefix:"__" name)

module GroupNode = struct
  type t =
    | Root
    | Cons of t * string

  let create parent name =
    if rep_ok name then
      Result.ok @@ Cons (parent, name)
    else
      Error (`Node_invariant name)

  let ( / ) = create

  let root = Root 

  let of_path = function
    | "/" -> Ok Root
    | str ->
      if not String.(starts_with ~prefix:"/" str) then
        Result.error @@
        `Node_invariant "path should start with a /"
      else if String.ends_with ~suffix:"/" str then
        Result.error @@
        `Node_invariant "path should not end with a /"
      else 
        let open Util.Result_syntax in
        List.fold_left
          (fun acc n -> acc >>= fun p -> create p n)
          (Ok Root) (List.tl @@ String.split_on_char '/' str)

  let name = function
    | Root -> ""
    | Cons (_, n) -> n

  let parent = function
    | Root -> None
    | Cons (parent, _) -> Some parent

  let rec ( = ) x y =
    match x, y with
    | Root, Root -> true
    | Root, Cons _ | Cons _, Root -> false
    | Cons (p, n), Cons (q, m) -> ( = ) p q && String.equal n m

  let rec fold f acc = function
    | Root -> f acc Root
    | Cons (parent, _) as p ->
      fold f (f acc p) parent

  let to_path = function
    | Root -> "/"
    | p ->
      fold (fun acc -> function
        | Root -> acc
        | Cons (_, n) -> "/" :: n :: acc) [] p
      |> String.concat ""

  let ancestors p =
    fold (fun acc -> function
      | Root -> acc
      | Cons (parent, _) -> parent :: acc) [] p

  let to_key p =
    let str = to_path p in
    String.(length str - 1 |> sub str 1)

  let to_prefix = function
    | Root -> ""
    | p -> to_key p ^ "/"

  let to_metakey p =
    to_prefix p ^ "zarr.json"

  let is_child_group x y =
    match x, y with
    | _, Root -> false
    | v, Cons (parent, _) -> parent = v

  let show = to_path

  let pp fmt t =
    Format.fprintf fmt "%s" @@ show t
end

module ArrayNode = struct
  type t =
    {parent : GroupNode.t
    ;name :  string}

  let create parent name =
    if rep_ok name then
      Result.ok @@ {parent; name}
    else
      Error (`Node_invariant name)

  let ( / ) = create

  let of_path p =
    match GroupNode.of_path p with
    | Error e -> Error e
    | Ok g ->
      match GroupNode.parent g with
      | Some parent ->
        Ok {parent; name = GroupNode.name g}
      | None ->
        Result.error @@
        `Node_invariant "Cannot create an array node from a root path"
      
  let ( = )
    {parent = p; name = n}
    {parent = q; name = m} = p = q && n = m

  let name = function
    | {parent = _; name} -> name

  let parent = function
    | {parent; _} -> parent

  let to_path = function
    | {parent = p; name} -> 
      if GroupNode.(p = root) then
        "/" ^ name
      else
        GroupNode.to_path p ^ "/" ^ name
  
  let ancestors = function
    | {parent; _} -> parent :: GroupNode.ancestors parent

  let is_parent x y =
    match x with
    | {parent = p; _} -> GroupNode.(p = y)

  let to_key = function
    | {parent; name} -> GroupNode.to_prefix parent ^ name

  let to_metakey p = to_key p ^ "/zarr.json"
  
  let show = to_path

  let pp fmt t =
    Format.fprintf fmt "%s" @@ show t
end
