CREATE USER etluser WITH PASSWORD 'etlpassword';

CREATE ROLE readonly;
create extension if not exists vector;

DO $$
BEGIN
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO readonly', current_database());
END
$$;
grant usage on schema public to readonly;
grant select on all tables in schema public to readonly;


alter default privileges in schema public grant select on tables to readonly; 

grant readonly to etluser;

create table if not exists users(
    id serial primary key, 
    first_name varchar(100), 
    last_name varchar(100),
    email varchar(255),
    created_at timestamp default current_timestamp, 
    updated_at timestamp default current_timestamp
);

create table if not exists items(
    id serial primary key, 
    name varchar(255) NOT NULL, 
    category varchar(100), 
    price decimal(10,2), 
    inventory int, 
    created_at timestamp default current_timestamp, 
    updated_at timestamp default current_timestamp
);


CREATE TABLE IF NOT EXISTS purchases
(
    id SERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    item_id BIGINT REFERENCES items(id),
    campaign_id VARCHAR(50),
    status SMALLINT DEFAULT 1,
    quantity INT DEFAULT 1,
    purchase_price DECIMAL(12,2),
    deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

grant select on table users to readonly; 
grant select on table items to readonly; 
grant select on table purchases to readonly;

create table if not exists reviews (
    review_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    date DATE,
    review TEXT,
    review_embedding vector(384)
);

grant select on table reviews to readonly;

-- Inserting some sample data into the reviews table
INSERT INTO public.reviews (customer_name, customer_email, date, review) VALUES
('Alice Smith', 'alice.smith@email.com', '2024-03-08', 'Amazing customer service! They went above and beyond to help me with my order. I will definitely be back.'),
('Bob Johnson', 'bob.johnson@email.com', '2024-03-07', 'Fast and efficient support. My question was answered quickly and the issue was resolved immediately.'),
('Charlie Brown', 'charlie.brown@email.com', '2024-03-06', 'Helpful and friendly staff. They were patient and understanding when I was having trouble navigating the website.'),
('David Garcia', 'david.garcia@email.com', '2024-03-05', 'Excellent communication! I received prompt updates on my order status and delivery time. I am really happy.'),
('Eve Williams', 'eve.williams@email.com', '2024-03-04', 'Very impressed with the customer service! They were quick to address my concerns and offered a satisfactory solution.'),
('Finn Miller', 'finn.miller@email.com', '2024-03-03', 'Positive experience dealing with customer support. They were knowledgeable and able to assist me with my product inquiry efficiently.'),
('Grace Davis', 'grace.davis@email.com', '2024-03-02', 'My query was acknowledged, and I received a response. The answer was okay, but I am still unsure about the overall experience.'),
('Harry Rodriguez', 'harry.rodriguez@email.com', '2024-03-01', 'Terrible customer service! I waited for hours for a reply to a question, and no one replied yet.'),
('Ivy Wilson', 'ivy.wilson@email.com', '2024-02-29', 'Extremely unhelpful and rude support staff. They were dismissive of my issue and offered no real assistance.'),
('Jack Moore', 'jack.moore@email.com', '2024-02-28', 'Disappointing customer service experience. I had to contact them multiple times to get a simple issue resolved, and they keep redirecting me.'),
('Lucas Bennett', 'lucas.bennett@email.com', '2024-03-02', 'Awful experience! The support team completely ignored my request for help, and I had to figure everything out on my own.'),
('Sophia Turner', 'sophia.turner@email.com', '2024-03-01', 'Horrible service. I was given misleading information that caused even more issues, and no one took responsibility for it.'),
('Ethan Brooks', 'ethan.brooks@email.com', '2024-02-29', 'Frustrating support process. I kept getting automated responses with no real solution, and it took days to get a proper answer.');