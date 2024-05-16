(lang dune 3.15)

(name zarr)

(generate_opam_files true)

(source
 (github zoj613/zarr-ml))

(authors "Author Name")

(maintainers "Maintainer Name")

(license BSD-3-Clause)

(documentation https://url/to/documentation)

(package
 (name zarr)
 (synopsis "A short synopsis")
 (description "A longer description")
 (depends
   dune
   (ocaml (>= 4.14.2))
   yojson
   ppx_derviving_yojson
   ezgzip
   checkseum
   stdint
   owl)
 (tags
  (topics "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
