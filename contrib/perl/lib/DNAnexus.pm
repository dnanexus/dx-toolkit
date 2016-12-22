# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

# ABSTRACT: DNAnexus Perl bindings

=head1 NAME

DNAnexus Perl bindings

=head1 SYNOPSIS

 use DNAnexus::API qw(:all);
 $res = DNAnexus::API::systemSearch({class=>"file", describe=>JSON::true});
 for $r (@{$$res{results}}) {
     ...
 }

=cut

package DNAnexus;

use strict;
use Exporter;
use LWP;
use HTTP::Request;
use JSON;

use DNAnexus::API;
use DNAnexus::Utils::Env qw(get_env);

our @ISA = "Exporter";
our @EXPORT_OK = qw(DXHTTPRequest);

our ($APISERVER_HOST, $APISERVER_PORT, $APISERVER_PROTOCOL, $APISERVER, $SECURITY_CONTEXT, $PROJECT_CONTEXT_ID, $_DEBUG);

sub DXHTTPRequest($;$%) {
    my ($resource, $data, %kwargs) = @_;
    $data ||= {};
    die("Expected data to be a hash") if ref($data) ne "HASH";

    my $method = $kwargs{method} || 'POST';
    my $headers = $kwargs{headers} || {};

    my $request = HTTP::Request->new($method, $APISERVER.$resource);
    $request->header($headers);
    $request->content_type('application/json');
    $request->authorization($$SECURITY_CONTEXT{auth_token_type}.' '.$$SECURITY_CONTEXT{auth_token});

    $request->content(encode_json($data));

    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);

    if ($kwargs{want_full_response}) {
        return $response;
    } else {
        for my $header (keys %{$response->headers}) {
            if (lc($header) eq 'content-type' and ${$response->headers}{$header} =~ /^application\/json/) {
                return decode_json($response->content);
            }
        }
        return $response->content;
    }
}

sub set_api_server_info(;$$$) {
    my ($host, $port, $protocol) = @_;
    $host ||= 'api.dnanexus.com';
    $port ||= 443;
    $protocol ||= 'https';

    $APISERVER_HOST = $host;
    $APISERVER_PORT = $port;
    $APISERVER = $protocol . "://" . $host . ":" . $port;
}

sub set_security_context($) {
    my ($sc) = @_;
    $SECURITY_CONTEXT = $sc;
}

sub _initialize() {
    print "DNAnexus initializing...";

    if (exists $ENV{DX_DEBUG}) {
        $_DEBUG = 1;
    }

    my $env_vars = get_env();
    for my $var (keys %$env_vars) {
        $ENV{$var} = $$env_vars{$var} unless exists $ENV{$var};
    }

    if (exists $ENV{DX_APISERVER_HOST} and exists $ENV{DX_APISERVER_PORT}) {
        set_api_server_info($ENV{DX_APISERVER_HOST}, $ENV{DX_APISERVER_PORT});
    } else {
        set_api_server_info();
    }

    if (exists $ENV{DX_SECURITY_CONTEXT}) {
        set_security_context(decode_json($ENV{DX_SECURITY_CONTEXT}));
    }

    # print $APISERVER_HOST, $APISERVER_PORT, $APISERVER_PROTOCOL, $APISERVER, $SECURITY_CONTEXT, $PROJECT_CONTEXT_ID, $_DEBUG;
}

_initialize();

1;
