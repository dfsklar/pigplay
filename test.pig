REGISTER SimpleTextStorer.jar;

NEW = LOAD 'newstate' using PigStorage(',') as (user:chararray,code:chararray);

STORE NEW INTO 'newnewnew' USING com.yahoo.pigudf.SimpleTextStorer('X');

