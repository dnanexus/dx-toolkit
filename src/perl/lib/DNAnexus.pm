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

our @ISA = "Exporter";
our @EXPORT_OK = qw(DXHTTPRequest);

our ($APISERVER_HOST, $APISERVER_PORT, $APISERVER, $SECURITY_CONTEXT);

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

if (exists $ENV{DX_APISERVER_HOST} and exists $ENV{DX_APISERVER_PORT}) {
    set_api_server_info($ENV{DX_APISERVER_HOST}, $ENV{DX_APISERVER_PORT});
} else {
    set_api_server_info();
}

if (exists $ENV{DX_SECURITY_CONTEXT}) {
    set_security_context(decode_json($ENV{DX_SECURITY_CONTEXT}));
}

1;
