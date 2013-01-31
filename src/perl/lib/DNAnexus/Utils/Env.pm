#!/usr/bin/env perl

use strict;
use File::HomeDir;
use File::Spec::Functions qw(catfile);

sub parse_env_file($) {
    my ($filename) = @_;
    my $env_vars;
    
    open(my $fd, '<', $filename) or return $env_vars;
    while (<$fd>) {
        if (/export (DX.+?)='(.+)'/) {
            $$env_vars{$1} = $2;
        }
    }
    close $fd;
    return $env_vars;
}

sub parse_user_env_file() {
    return parse_env_file(catfile(File::HomeDir::home(), '.dnanexus_config', 'environment'));
}

sub parse_installed_env_file() {
    return parse_env_file(catfile('opt', 'dnanexus', 'environment'));
}

sub get_env(;$) {
    my ($suppress_warning) = @_;
    my %env_vars;
    my @recognized_vars = qw(DX_APISERVER_HOST DX_APISERVER_PORT DX_APISERVER_PROTOCOL DX_PROJECT_CONTEXT_ID DX_WORKSPACE_ID DX_CLI_WD DX_USERNAME DX_PROJECT_CONTEXT_NAME DX_SECURITY_CONTEXT);
    for my $var (@recognized_vars) {
        if (exists $ENV{$var}) {
            $env_vars{$var} = $ENV{$var};
        }
    }

    my $user_file_env_vars = parse_user_env_file();
    my $installed_file_env_vars = parse_installed_env_file();

    for my $var (@recognized_vars) {
        next if exists $env_vars{$var};
        if (defined $$user_file_env_vars{$var}) {
            $env_vars{$var} = $$user_file_env_vars{$var};
        } elsif (defined $$installed_file_env_vars{$var}) {
            $env_vars{$var} = $$installed_file_env_vars{$var};
        }
    }

    for my $standalone_var ('DX_CLI_WD', 'DX_USERNAME', 'DX_PROJECT_CONTEXT_NAME') {
        next if exists $env_vars{$standalone_var};
        open(my $fh, '<', catfile(File::HomeDir::home(), '.dnanexus_config', $standalone_var));
        $env_vars{$standalone_var} = <$fh>;
        close $fh;
    }

    if (-t STDOUT) {
        my @already_set = ();
        for my $var (keys %$user_file_env_vars) {
            if (defined $env_vars{$var} and $$user_file_env_vars{$var} ne $env_vars{$var}) {
                push(@already_set, $var);
            }
        }

        if (scalar(@already_set) > 0 and not $suppress_warning) {
            warn "WARNING: The following environment variables were found to be different than the values last stored by dx: ".join(", ", @already_set)."\n";
            warn "To use the values stored by dx, unset the environment variables in your shell by running \"source ~/.dnanexus_config/unsetenv\".  To clear the dx-stored values, run \"dx clearenv\"\n";
        }
    }
    return \%env_vars;
}

1;
