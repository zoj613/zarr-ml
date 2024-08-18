type 'a t = 'a
let return x = x
let iter = List.iter
let iter_s = iter
let fold_left = List.fold_left
let map = List.map
let concat_map = List.concat_map

module Infix = struct
  let (>>=) x f = f x
  let (>>|) = (>>=)
end
