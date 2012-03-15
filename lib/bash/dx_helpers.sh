#!/bin/bash
# DNAnexus helper library

# Portions Copyright 1999-2011 Gentoo Foundation
# Distributed under the terms of the GNU General Public License v2


unpack() {
	local x
	local y
	local myfail
	[ -z "$*" ] && die "Nothing passed to the 'unpack' command"

	for x in "$@"; do
		vecho ">>> Unpacking ${x} to ${PWD}"
		y=${x%.*}
		y=${y##*.}

		[[ ! -s ${x} ]] && die "${x} does not exist"

		_unpack_tar() {
			if [ "${y}" == "tar" ]; then
				$1 -c -- "$x" | tar xof -
				assert_sigpipe_ok "$myfail"
			else
				local cwd_dest=${x##*/}
				cwd_dest=${cwd_dest%.*}
				$1 -c -- "${srcdir}${x}" > "${cwd_dest}" || die "$myfail"
			fi
		}

		myfail="failure unpacking ${x}"
		case "${x##*.}" in
			tar)
				tar xof "$x" || die "$myfail"
				;;
			tgz)
				tar xozf "$x" || die "$myfail"
				;;
			tbz|tbz2)
				${PORTAGE_BUNZIP2_COMMAND:-${PORTAGE_BZIP2_COMMAND} -d} -c -- "$x" | tar xof -
				assert_sigpipe_ok "$myfail"
				;;
			ZIP|zip|jar)
				# unzip will interactively prompt under some error conditions,
				# as reported in bug #336285
				( while true ; do echo n || break ; done ) | \
				unzip -qo "${srcdir}${x}" || die "$myfail"
				;;
			gz|Z|z)
				_unpack_tar "gzip -d"
				;;
			bz2|bz)
				_unpack_tar "${PORTAGE_BUNZIP2_COMMAND:-${PORTAGE_BZIP2_COMMAND} -d}"
				;;
			7Z|7z)
				local my_output
				my_output="$(7z x -y "${srcdir}${x}")"
				if [ $? -ne 0 ]; then
					echo "${my_output}" >&2
					die "$myfail"
				fi
				;;
			RAR|rar)
				unrar x -idq -o+ "${srcdir}${x}" || die "$myfail"
				;;
			LHa|LHA|lha|lzh)
				lha xfq "${srcdir}${x}" || die "$myfail"
				;;
			a)
				ar x "${srcdir}${x}" || die "$myfail"
				;;
			deb)
				# Unpacking .deb archives can not always be done with
				# `ar`.  For instance on AIX this doesn't work out.  If
				# we have `deb2targz` installed, prefer it over `ar` for
				# that reason.  We just make sure on AIX `deb2targz` is
				# installed.
				if type -P deb2targz > /dev/null; then
					y=${x##*/}
					local created_symlink=0
					if [ ! "$x" -ef "$y" ] ; then
						# deb2targz always extracts into the same directory as
						# the source file, so create a symlink in the current
						# working directory if necessary.
						ln -sf "$x" "$y" || die "$myfail"
						created_symlink=1
					fi
					deb2targz "$y" || die "$myfail"
					if [ $created_symlink = 1 ] ; then
						# Clean up the symlink so the ebuild
						# doesn't inadvertently install it.
						rm -f "$y"
					fi
					mv -f "${y%.deb}".tar.gz data.tar.gz || die "$myfail"
				else
					ar x "$x" || die "$myfail"
				fi
				;;
			lzma)
				_unpack_tar "lzma -d"
				;;
			xz)
			        _unpack_tar "xz -d"
				;;
			*)
				vecho "unpack ${x}: file format not recognized. Ignoring."
				;;
		esac
	done
	# Do not chmod '.' since it's probably ${WORKDIR} and PORTAGE_WORKDIR_MODE
	# should be preserved.
	find . -mindepth 1 -maxdepth 1 ! -type l -print0 | \
		xargs -r -0 chmod -fR a+rX,u+w,g-w,o-w
}

die() {
	set +e
	local n filespacing=0 linespacing=0
	dxerror "  $(printf "%${filespacing}s" "${BASH_SOURCE[1]##*/}"), line $(printf "%${linespacing}s" "${BASH_LINENO[0]}"): $@"
	exit 1
}

assert_sigpipe_ok() {
	# When extracting a tar file like this:
	#
	#     bzip2 -dc foo.tar.bz2 | tar xof -
	#
	# For some tar files (see bug #309001), tar will
	# close its stdin pipe when the decompressor still has
	# remaining data to be written to its stdout pipe. This
	# causes the decompressor to be killed by SIGPIPE. In
	# this case, we want to ignore pipe writers killed by
	# SIGPIPE, and trust the exit status of tar. We refer
	# to the bash manual section "3.7.5 Exit Status"
	# which says, "When a command terminates on a fatal
	# signal whose number is N, Bash uses the value 128+N
	# as the exit status."

	local x pipestatus=${PIPESTATUS[*]}
	for x in $pipestatus ; do
		# Allow SIGPIPE through (128 + 13)
		[[ $x -ne 0 && $x -ne ${PORTAGE_SIGPIPE_STATUS:-141} ]] && die "$@"
	done

	# Require normal success for the last process (tar).
	[[ $x -eq 0 ]] || die "$@"
}

unset_colors() {
	COLS=80
	ENDCOL=

	GOOD=
	WARN=
	BAD=
	NORMAL=
	HILITE=
	BRACKET=
}

set_colors() {
	COLS=${COLUMNS:-0}      # bash's internal COLUMNS variable
	(( COLS == 0 )) && COLS=$(set -- $(stty size 2>/dev/null) ; echo $2)
	(( COLS > 0 )) || (( COLS = 80 ))

	# Now, ${ENDCOL} will move us to the end of the
	# column;  irregardless of character width
	ENDCOL=$'\e[A\e['$(( COLS - 8 ))'C'
	GOOD=$'\e[32;01m'
	WARN=$'\e[33;01m'
	BAD=$'\e[31;01m'
	HILITE=$'\e[36;01m'
	BRACKET=$'\e[34;01m'
	NORMAL=$'\e[0m'
}

RC_ENDCOL="yes"
RC_INDENTATION=''
RC_DEFAULT_INDENT=2
RC_DOT_PATTERN=''

dxinfo() {
	echo -e "$@" | while read -r ; do
		echo " $GOOD*$NORMAL $REPLY"
	done
	return 0
}

dxwarn() {
	echo -e "$@" | while read -r ; do
		echo " $WARN*$NORMAL $RC_INDENTATION$REPLY" >&2
	done
	return 0
}

dxerror() {
	echo -e "$@" | while read -r ; do
		echo " $BAD*$NORMAL $RC_INDENTATION$REPLY" >&2
	done
	return 0
}

vecho() {
	quiet_mode || echo "$@"
}

quiet_mode() {
        [[ ${DX_QUIET} -eq 1 ]]
}
