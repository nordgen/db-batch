--.load ./tests/Support/Data/libsqlite_hashes.so
--SELECT load_extension('ext/misc/sha1');
--SELECT load_extension('libsqlite_hashes');

CREATE TABLE profiles (
    profile_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
);

CREATE TABLE users (
    user_id INTEGER,
    username TEXT NULL,
    password TEXT NULL,
    profile_id INTEGER NULL,
    PRIMARY KEY (user_id),
    FOREIGN KEY (profile_id)
        REFERENCES profiles (profile_id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
);


-- Add a few profiles to the table.
REPLACE INTO `profiles` (`profile_id`, `first_name`, `last_name`, `email`) VALUES
(1, 'Bob', 'Ross', 'bob.ross@example.com'),
(2, 'Fred', 'Flintstone', 'fred.flintsone@example.com');

-- Add a few users to the table.
REPLACE INTO `users` (`user_id`, `username`, `password`, `profile_id`) VALUES
(1, 'Bob', /*hash_sha256(*/'Ross'/*)*/, 1),
(2, 'Fred', /*hash_sha256(*/'Flintstone'/*)*/, 1);


