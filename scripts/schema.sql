CREATE TABLE users (
  id INTEGER(11) AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

CREATE TABLE addresses (
  user_id INTEGER(11) NOT NULL,
  address VARCHAR(255) NOT NULL,

  FOREIGN KEY user_fk (user_id)
   REFERENCES users (id)
           ON DELETE CASCADE
           ON UPDATE CASCADE
);
