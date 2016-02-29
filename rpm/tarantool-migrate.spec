Name: tarantool-migrate
Version: 1.0.1
Release: 1%{?dist}
Summary: Templates for Tarantool modules
Group: Applications/Databases
License: BSD
URL: https://github.com/bigbes/migrate
Source0: https://github.com/tarantool/%{name}/archive/%{version}/%{name}-%{version}.tar.gz
BuildRequires: cmake >= 2.8
BuildRequires: gcc >= 4.5
BuildRequires: tarantool-devel >= 1.6.8.0
BuildRequires: msgpuck-devel >= 1.0.2
BuildRequires: small-devel >= 1.0.1
BuildRequires: /usr/bin/prove
Requires: small >= 1.0.1
# Requires: tarantool >= 1.6.8.0

%description
This package provides a set of Lua, Lua/C and C module templates for Tarantool.

%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo
make %{?_smp_mflags}

%install
%make_install

%files
%{_libdir}/tarantool/*/
%{_datarootdir}/tarantool/*/
%doc README.md

%changelog
* Mon Feb 29 2016 Eugine Blikh <bigbes@tarantool.org> 1.0.0-1
- Initial version of the RPM spec
