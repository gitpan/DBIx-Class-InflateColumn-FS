package DBIx::Class::InflateColumn::FS;

use strict;
use warnings;
use DBIx::Class::UUIDColumns;
use File::Spec;
use File::Path;
use File::Copy ();
use Path::Class;

our $VERSION = '0.01003';

=head1 NAME

DBIx::Class::InflateColumn::FS - store BLOBs in the file system

=head1 SYNOPSIS

  __PACKAGE__->load_components('InflateColumn::FS Core');
  __PACKAGE__->add_columns(
      id => {
          data_type         => 'INT',
          is_auto_increment => 1,
      },
      file => {
          data_type => 'TEXT',
          is_fs_column => 1,
          fs_column_path => '/var/lib/myapp/myfiles',
      },
      file_2 => {
          data_type => 'TEXT',
          is_fs_column => 1,
          fs_column_path => '/var/lib/myapp/myfiles',
          fs_new_on_update => 1
      },
  );
  __PACKAGE__->set_primary_key('id');

  # in application code
  $rs->create({ file => $file_handle });

  $row = $rs->find({ id => $id });
  my $fh = $row->file->open('r');

=head1 DESCRIPTION

Provides inflation to a Path::Class::File object allowing file system storage
of BLOBS.

The storage path is specified with C<fs_column_path>.  Each file receives a
unique name, so the storage for all FS columns can share the same path.

Within the path specified by C<fs_column_path>, files are stored in
sub-directories based on the first 2 characters of the unique file names.  Up to
256 sub-directories will be created, as needed.  Override C<_fs_column_dirs> in
a derived class to change this behavior.

C<fs_new_on_update> will create a new file name if the file has been updated.

=cut

=head1 METHODS

=cut

=head2 register_column

=cut

sub register_column {
    my ($self, $column, $info, @rest) = @_;
    $self->next::method($column, $info, @rest);
    return unless defined($info->{is_fs_column});

    $self->inflate_column($column => {
        inflate => sub { 
            my ($value, $obj) = @_;
            $obj->_inflate_fs_column($column, $value);
        },
        deflate => sub {
            my ($value, $obj) = @_;
            $obj->_deflate_fs_column($column, $value);
        },
    });
}

=head2 fs_file_name

Provides the file naming algorithm.  Override this method to change it.

This method is called with two parameters: The name of the column and the
C<< column_info >> object.

=cut

sub fs_file_name {
    my ($self, $column, $column_info) = @_;
    return DBIx::Class::UUIDColumns->get_uuid;
}

sub _fs_column_storage {
    my ( $self, $column, $deflate ) = @_;

    my $column_info = $self->result_source->column_info($column);
    $self->throw_exception("$column is not an fs_column")
        unless $column_info->{is_fs_column};

    if ( (!$column_info->{fs_new_on_update} || !$deflate) && ( my $filename = $self->{_column_data}{$column} ) ) {
        return Path::Class::File->new($column_info->{fs_column_path}, $filename);
    }
    else {
        $filename = $self->fs_file_name($column, $column_info);
        return Path::Class::File->new(
            $column_info->{fs_column_path},
            $self->_fs_column_dirs($filename),
            $filename
        );
    }
}

=head2 _fs_column_dirs

Returns the sub-directory components for a given file name.  Override it to
provide a deeper directory tree or change the algorithm.

=cut

sub _fs_column_dirs {
    shift;
    my $filename = shift;

    return $filename =~ /(..)/;
}

=head2 copy

Copies a row object, duplicating the files backing fs columns.

=cut

sub copy {
    my ($self, $changes) = @_;

    $changes ||= {};
    my $col_data     = { %{$self->{_column_data}} };

    foreach my $col ( keys %$col_data ) {
        my $column_info = $self->result_source->column_info($col);
        if ( $column_info->{is_fs_column}
             && defined $col_data->{$col} ) {  # nothing special required for NULLs
            $col_data->{$col} = undef;
            
            # pass the original file to produce a copy on deflate
            my $accessor = $column_info->{accessor} || $col;
            $changes->{$col} ||= $self->$accessor;
        }
    }

    my $temp = bless { _column_data => $col_data }, ref $self;
    $temp->result_source($self->result_source);

    my $copy = $temp->next::method($changes);

    # force reinflation of fs colmuns on next access
    delete $copy->{_inflated_column}{$_}
        for grep { $self->result_source->column_info($_)->{is_fs_column} }
            keys %$col_data;

   return $copy;
}

=head2 delete

Deletes the associated file system storage when a row is deleted.

=cut

sub delete {
    my ( $self, @rest ) = @_;

    for ( $self->columns ) {
        if ( $self->result_source->column_info($_)->{is_fs_column} ) {
            next unless $self->$_;
            $self->$_->remove;
        }
    }

    return $self->next::method(@rest);
}

=head2 update

Deletes the associated file system storage when a column is set to null.

=cut

sub update {
    my ($self, $upd) = @_;

    my %changed = ($self->get_dirty_columns, %{$upd || {}});

    # cache existing fs_colums before update so we can delete storge
    # afterwards if necessary
    my $s = $self->result_source;
    my %fs_column =
        map  { ($_, $self->$_) }
        grep { $s->column_info($_)->{is_fs_column} }
        keys %changed;

    # attempt super update, first, so it can throw on DB errors
    # and perform other checks
    $self->next::method($upd);

    while ( my ($column, $value) = each %changed ) {
        if ( $s->column_info($column)->{is_fs_column} ) {
            # remove the storage if the column was set to NULL
            $fs_column{$column}->remove if !defined $value;

            # force reinflation on next access
            delete $self->{_inflated_column}{$column};
        }
    }
    return $self;
}

=head2 _inflate_fs_column

Inflates a file column to a Path::Class::File object.

=cut

sub _inflate_fs_column {
    my ( $self, $column, $value ) = @_;
    return unless defined $value;

    return $self->_fs_column_storage($column);
}

=head2 _deflate_fs_column

Deflates a file column to its storage path name, relative to C<fs_column_path>.
In the database, a file column is just a place holder for inflation/deflation.
The actual file lives in the file system.

=cut

sub _deflate_fs_column {
    my ( $self, $column, $value ) = @_;
    
    # already deflated?
    return $value unless ref $value;
    my $fs_new_on_update = $self->result_source->column_info($column)->{fs_new_on_update};
    my $file = $self->_fs_column_storage($column, 1);
    
    if ( $fs_new_on_update && (my $oldfile = $self->{_column_data}{$column}) ) {
        my $column_info = $self->result_source->column_info($column);
        Path::Class::File->new($column_info->{fs_column_path}, $oldfile)->remove;
    }
    
    if ( $fs_new_on_update || $value ne $file ) {
        File::Path::mkpath([$file->dir]);

        # get a filehandle if we were passed a Path::Class::File
        my $fh1 = eval { $value->openr } || $value;
        my $fh2 = $file->openw or die;
        File::Copy::copy($fh1, $fh2);

        # force re-inflation on next access
        delete $self->{_inflated_column}{$column};
    }
    my $basename = $file->basename;
    return File::Spec->catfile($self->_fs_column_dirs($basename), $basename);
}

=head2 table

Overridden to provide a hook for specifying the resultset_class.  If
you provide your own resultset_class, inherit from
InflateColumn::FS::ResultSet.

=cut

sub table {
    my $self = shift;

    my $ret = $self->next::method(@_);
    if ( @_ && $self->result_source_instance->resultset_class
               eq 'DBIx::Class::ResultSet' ) {
        $self->result_source_instance
             ->resultset_class('DBIx::Class::InflateColumn::FS::ResultSet');
    }
    return $ret;
}

=head1 SUPPORT

Community support can be found via:

  Mailing list: http://lists.scsys.co.uk/mailman/listinfo/dbix-class/

  IRC: irc.perl.org#dbix-class

The author is C<semifor> on IRC and a member of the mailing list.

=head1 AUTHOR

semifor: Marc Mims <marc@questright.com>

=head1 LICENSE

You may distribute this code under the same terms as Perl itself.

=cut

1;
