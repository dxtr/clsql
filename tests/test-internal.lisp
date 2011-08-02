;;;; -*- Mode: LISP; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;; *************************************************************************
;;;; FILE IDENTIFICATION
;;;;
;;;; Name:    test-internal.lisp
;;;; Purpose: Tests for internal clsql functions
;;;; Author:  Kevin M. Rosenberg
;;;; Created: May 2004
;;;;
;;;; This file, part of CLSQL, is Copyright (c) 2004-2010 by Kevin M. Rosenberg
;;;;
;;;; CLSQL users are granted the rights to distribute and use this software
;;;; as governed by the terms of the Lisp Lesser GNU Public License
;;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;; *************************************************************************

(in-package #:clsql-tests)
(clsql-sys:file-enable-sql-reader-syntax)

(setq *rt-internal*
  '(
    (deftest :int/convert/1
        (clsql-sys::prepared-sql-to-postgresql-sql "SELECT FOO FROM BAR")
      "SELECT FOO FROM BAR")

    (deftest :int/convert/2
        (clsql-sys::prepared-sql-to-postgresql-sql "SELECT FOO FROM BAR WHERE ID=?")
      "SELECT FOO FROM BAR WHERE ID=$1")

    (deftest :int/convert/3
        (clsql-sys::prepared-sql-to-postgresql-sql "SELECT FOO FROM \"BAR\" WHERE ID=? AND CODE=?")
      "SELECT FOO FROM \"BAR\" WHERE ID=$1 AND CODE=$2")

    (deftest :int/convert/4
        (clsql-sys::prepared-sql-to-postgresql-sql "SELECT FOO FROM BAR WHERE ID=\"Match?\" AND CODE=?")
      "SELECT FOO FROM BAR WHERE ID=\"Match?\" AND CODE=$1")

    (deftest :int/convert/5
        (clsql-sys::prepared-sql-to-postgresql-sql "SELECT 'FOO' FROM BAR WHERE ID='Match?''?' AND CODE=?")
      "SELECT 'FOO' FROM BAR WHERE ID='Match?''?' AND CODE=$1")

    (deftest :int/output-caching/1
     ;; ensure that key generation and matching is working
     ;; so that this table doesnt balloon (more than designed)
     (list
      (progn (clsql:sql [foo])
             (clsql:sql [foo])
             (hash-table-count clsql-sys::*output-hash*))

      (progn (clsql:sql [foo.bar])
             (clsql:sql [foo bar])
             (hash-table-count clsql-sys::*output-hash*))
      (progn (clsql:sql (clsql-sys:sql-expression
                         :table (clsql-sys::database-identifier 'foo)
                         :attribute (clsql-sys::database-identifier 'bar)))
             (clsql:sql (clsql-sys:sql-expression
                         :table (clsql-sys::database-identifier 'foo)
                         :attribute (clsql-sys::database-identifier 'bar)))
             (hash-table-count clsql-sys::*output-hash*)))
     (1 2 2))

    (deftest :int/output-caching/2
     ;; ensure that we can disable the output cache and
     ;; still have everything work
     (let ((clsql-sys::*output-hash*))
       (list (clsql:sql [foo]) (clsql:sql [foo]) (clsql:sql [foo.bar])))
     ("FOO" "FOO" "FOO.BAR"))

    ))

