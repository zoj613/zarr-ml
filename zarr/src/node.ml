type error = [ `Node_invariant | `Cannot_rename_root | `Invalid_path ]

(* Check if the path's name satisfies path invariants *)
let rep_ok name =
  (String.empty <> name) &&
  not (String.contains name '/') &&
  not (String.for_all (Char.equal '.') name) &&
  not (String.starts_with ~prefix:"__" name)

module Group = struct
  type t = Root | Cons of t * string

  let create parent name = match rep_ok name with
    | false -> Error `Node_invariant
    | true -> Ok (Cons (parent, name))

  let create' = Fun.flip create

  let of_path p = match String.split_on_char '/' p with
    | [""; ""] -> Ok Root
    | [_] -> Error `Invalid_path  (* occurs if path is empty or a single string with no '/' character. *)
    | x :: _ when x <> String.empty -> Error `Invalid_path  (* occurs if path does not start with '/'. *)
    | xs -> List.fold_left (fun a n -> Result.bind a (create' n)) (Ok Root) (List.tl xs)
 
  let name = function
    | Root -> ""
    | Cons (_, n) -> n

  let parent = function
    | Root -> None
    | Cons (parent, _) -> Some parent

  let rec ( = ) x y = match x, y with
    | Root, Root -> true
    | Root, Cons _ | Cons _, Root -> false
    | Cons (p, n), Cons (q, m) -> ( = ) p q && String.equal n m

  let rec fold f acc = function
    | Root -> f acc Root
    | Cons (parent, _) as p -> fold f (f acc p) parent

  let prepend_name acc = function
    | Root -> acc
    | Cons (_, n) -> "/" :: n :: acc

  let to_path = function
    | Root -> "/"
    | p -> String.concat "" (fold prepend_name [] p)

  let prepend_node acc = function
    | Root -> acc
    | Cons (p, _) -> p :: acc

  let to_key p =
    let str = to_path p in
    String.sub str 1 (String.length str - 1)

  let to_prefix = function
    | Root -> ""
    | p -> to_key p ^ "/"

  let is_child_group x y = match x, y with
    | _, Root -> false
    | v, Cons (parent, _) -> parent = v

  let rename t str = match t with
    | Cons (parent, _) when rep_ok str -> Ok (Cons (parent, str))
    | Cons _ -> Error `Node_invariant
    | Root -> Error `Cannot_rename_root

  let root = Root 
  let ( / ) = create
  let show = to_path
  let ancestors p = fold prepend_node [] p
  let pp fmt t = Format.fprintf fmt "%s" (show t)
  let to_metakey p = to_prefix p ^ "zarr.json"
end

module Array = struct
  type t = {parent : Group.t option; name : string}

  let of_path p = match Group.of_path p with
    | Error _ as e -> e
    | Ok g -> match Group.parent g with
      | Some _ as parent -> Ok {parent; name = Group.name g}
      | None -> Error `Node_invariant

  let to_path {parent = p; name} = match p with
    | None -> "/"
    | Some g when Group.(g = root) -> "/" ^ name
    | Some g -> Group.to_path g ^ "/" ^ name
  
  let ancestors {parent; _} = match parent with
    | None -> []
    | Some g -> g :: Group.ancestors g

  let is_parent {parent; _} y = match parent with
    | None -> false
    | Some g -> Group.(g = y)

  let to_key {parent; name} = match parent with
    | Some g -> Group.to_prefix g ^ name
    | None -> "" 

  let to_metakey = function
    | {parent = None; _} -> "zarr.json"
    | p -> to_key p ^ "/zarr.json"

  let rename t name = match t.parent with
    | Some _ when rep_ok name -> Ok {t with name}
    | Some _ -> Error `Node_invariant
    | None -> Error `Cannot_rename_root
      
  let create g name = match rep_ok name with
    | true -> Ok {parent = Some g; name}
    | false -> Error `Node_invariant

  let ( / ) = create
  let show = to_path
  let root = {parent = None; name = ""}
  let ( = ) {parent = p; name = n} {parent = q; name = m} = p = q && n = m
  let parent {parent = p; _} = p
  let name {parent = _; name = n} = n
  let pp fmt t = Format.fprintf fmt "%s" (show t)
end
