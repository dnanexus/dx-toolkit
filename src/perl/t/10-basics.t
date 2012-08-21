#!/usr/bin/env perl

use Test::More;

use DNAnexus::API;

$f = DNAnexus::API::systemFindDataObjects();
for $i (@{$$f{results}}) {
    print "$_\t$$i{$_}\n" for keys %$i;
}

ok( 1 == 1 );

done_testing();

