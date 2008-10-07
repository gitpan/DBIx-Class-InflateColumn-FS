#!perl -wT
use warnings;
use strict;
use DBICx::TestDatabase;
use Test::More tests => 13;
use Path::Class qw/file/;
use File::Compare;
use lib qw(t/lib);

my $schema = DBICx::TestDatabase->new('My::TestSchema');
my $rs = $schema->resultset('Book');

# we'll use *this* file as our content
# TODO: Copy it or create something else so errant tests don't inadvertently
# delete it!
my $file = file($0);

my $book = $rs->create({
    name => 'Alice in Wonderland',
    cover_image => $file,
});

isa_ok( $book->cover_image, 'Path::Class::File' );
isnt( $book->cover_image, $file, 'storage is a different file' );
ok( compare($book->cover_image, $file) == 0, 'file contents equivalent');

# setting a file to itself should be a no-op
my $storage = Path::Class::File->new($book->cover_image);
$book->update({ cover_image => $storage });

is( $storage, $book->cover_image, 'setting storage to self' );

# deleting the row should delete the associated file
$book->delete;
ok( ! -e $storage, 'file successfully deleted' );

# multiple rows
my ($book1, $book2) = map {
    $rs->create({ name => $_, cover_image => $file })
} qw/Book1 Book2/;

isnt( $book1->cover_image, $book2->cover_image, 'rows have different storage' );

$rs->delete;
ok ( ! -e $book1->cover_image, "storage deleted for row 1" );
ok ( ! -e $book2->cover_image, "storage deleted for row 2" );


# null fs_column
$book = $rs->create({ name => 'No cover image', cover_image => undef });

ok ( !defined $book->cover_image, 'null fs_column' );


# file handle
open my $fh, '<', $0 or die "failed to open $0 for read: $!\n";

$book->cover_image($fh);
$book->update;
close $fh or die;

ok( compare($book->cover_image, $0) == 0, 'store from filehandle' );

# setting fs_column to null should delete storage
$book = $rs->create({ name => 'Here today, gone tomorrow',
        cover_image => $file });
$storage = $book->cover_image;
ok( -e $storage, 'storage exists before nulling' );
$book->update({ cover_image => undef });
ok( ! -e $storage, 'does not exist after nulling' );

$book->update({ cover_image => $file });
$book->update({ id => 999 });
$book->discard_changes;
ok( -e $book->cover_image, 'storage renamed on PK change' );
