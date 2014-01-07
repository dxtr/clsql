;;;; -*- Mode: LISP; Syntax: ANSI-Common-Lisp; Base: 10 -*-

(in-package #:clsql-sys)


(defmethod database-pkey-constraint ((class standard-db-class)
				     (database clsql-sqlite3:sqlite3-database))
  (let* ((keys (keyslots-for-class class))
         (cons (when (= 1 (length keys))
                 (view-class-slot-db-constraints (first keys)))))
    ;; This method generates primary key constraints part of the table
    ;; definition. For Sqlite autoincrement primary keys to work properly
    ;; this part of the table definition must be left out (IFF autoincrement) .
    (when (or (null cons) ;; didnt have constraints to check
              ;; didnt have auto-increment
              (null (intersection
                     +auto-increment-names+
                     (listify cons))))
      (call-next-method))))

