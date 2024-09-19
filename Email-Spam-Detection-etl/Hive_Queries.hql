-- Integrating data from the S3 bucket

CREATE EXTERNAL TABLE spam_ham_table (    
    label BIGINT,
    subject STRING,
    email_to STRING,
    email_from STRING,
    message STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://ca675-bucket/data/';

LOAD DATA INPATH 's3://ca675-bucket/data/processed_data.csv' INTO TABLE spam_ham_table;


-- creating Spam and Ham tables.

CREATE TABLE classified_emails1 AS
SELECT
  shp.label,
  shp.subject,
  shp.email_to,
  shp.email_from,
  shp.message,
  CASE
    WHEN b.word IS NOT NULL THEN 1  -- spam
    ELSE 0  -- ham
  END AS is_spam
FROM
  spam_ham_processed1 shp
LEFT JOIN
  bag_of_words1 b
ON
  shp.message LIKE CONCAT('%', b.word, '%'); -- a pattern to match strings in message column with the keywords in bag_of_words1 table


  -- Now I'll create two seperate tables - spam and ham

CREATE TABLE spam_emails AS
SELECT *
FROM classified_emails1
WHERE is_spam = 1 ;

CREATE TABLE ham_emails AS
SELECT *
FROM classified_emails1
WHERE is_spam = 0 ;


-- I've created a new table and passed conditions to remove null values and regular expressions.

CREATE TABLE IF NOT EXISTS spam_ham_processed1  
    STORED AS PARQUET AS
    SELECT *
    FROM spam_ham_table
    WHERE subject IS NOT NULL 
     AND message IS NOT NULL 
     AND subject NOT LIKE '%>+=+%' 
     AND email_from RLIKE '^([0-9]|[a-z]|[A-Z])'
     AND email_to RLIKE '^([0-9]|[a-z]|[A-Z])';

-- To check if there are any null values after creating the table
SELECT
  COUNT(*) > 0 AS has_null_values
FROM spam_ham_processed1
WHERE subject IS NULL AND email_from IS NULL AND email_to IS NULL message IS NULL;

-- if output : False then there are no null values present.


SELECT *
 FROM spam_ham_processed1
 LIMIT 10;



-- Created bag of words from : https://www.activecampaign.com/blog/spam-words
CREATE TABLE bag_of_words1 (
  id INT,
  word VARCHAR(255)
);


INSERT INTO bag_of_words1 (id, word)
VALUES
(1, '100% more'),
(2, 'additional income'),
(3, 'be your own boss'),
(4, 'best price'),
(5, 'big bucks'),
(6, 'billion'),
(7, 'cash bonus'),
(8, 'cents on the dollar'),
(9, 'consolidate debt'),
(10, 'double your cash'),
(11, 'double your income'),
(12, 'earn extra cash'),
(13, 'earn money'),
(14, 'eliminate bad credit'),
(15, 'extra cash'),
(16, 'extra income'),
(17, 'expect to earn'),
(18, 'fast cash'),
(19, 'financial freedom'),
(20, 'free access'),
(21, 'free consultation'),
(22, 'free gift'),
(23, 'free hosting'),
(24, 'free info'),
(25, 'free investment'),
(26, 'free membership'),
(27, 'free money'),
(28, 'free preview'),
(29, 'free quote'),
(30, 'free trial'),
(31, 'full refund'),
(32, 'get out of debt'),
(33, 'get paid'),
(34, 'giveaway'),
(35, 'guaranteed'),
(36, 'increase sales'),
(37, 'increase traffic'),
(38, 'incredible deal'),
(39, 'lower rates'),
(40, 'lowest price'),
(41, 'make money'),
(42, 'million dollars'),
(43, 'miracle'),
(44, 'money back'),
(45, 'once in a lifetime'),
(46, 'one time'),
(47, 'pennies a day'),
(48, 'potential earnings'),
(49, 'prize'),
(50, 'promise'),
(51, 'pure profit'),
(52, 'risk-free'),
(53, 'satisfaction guaranteed'),
(54, 'save big money'),
(55, 'save up to'),
(56, 'special promotion'),
(57, 'act now'),
(58, 'apply now'),
(59, 'become a member'),
(60, 'call now'),
(61, 'click below'),
(62, 'click here'),
(63, 'get it now'),
(64, 'do it today'),
(65, 'don’t delete'),
(66, 'exclusive deal'),
(67, 'get started now'),
(68, 'important information regarding'),
(69, 'information you requested'),
(70, 'instant'),
(71, 'limited time'),
(72, 'new customers only'),
(73, 'order now'),
(74, 'please read'),
(75, 'see for yourself'),
(76, 'sign up free'),
(77, 'take action'),
(78, 'this won’t last'),
(79, 'urgent'),
(80, 'what are you waiting for?'),
(81, 'while supplies last'),
(82, 'will not believe your eyes'),
(83, 'winner'),
(84, 'winning'),
(85, 'you are a winner'),
(86, 'you have been selected'),
(87, 'bulk email'),
(88, 'buy direct'),
(89, 'cancel at any time'),
(90, 'check or money order'),
(91, 'congratulations'),
(92, 'confidentiality'),
(93, 'cures'),
(94, 'dear friend'),
(95, 'direct email'),
(96, 'direct marketing'),
(97, 'hidden charges'),
(98, 'human growth hormone'),
(99, 'internet marketing'),
(100, 'lose weight'),
(101, 'mass email'),
(102, 'Meet singles'),
(103, 'Multi-level marketing'),
(104, 'No catch'),
(105, 'No cost'),
(106, 'No credit check'),
(107, 'No fees'),
(108, 'No gimmick'),
(109, 'No hidden costs'),
(110, 'No hidden fees'),
(111, 'No interest'),
(112, 'No investment'),
(113, 'No obligation'),
(114, 'No purchase necessary'),
(115, 'No questions asked'),
(116, 'No strings attached'),
(117, 'Not junk'),
(118, 'Not spam'),
(119, 'Obligation'),
(120, 'Passwords'),
(121, 'Requires initial investment'),
(122, 'Social security number'),
(123, 'This isn’t a scam'),
(124, 'This isn’t junk'),
(125, 'This isn’t spam'),
(126, 'Undisclosed'),
(127, 'Unsecured credit'),
(128, 'Unsecured debt'),
(129, 'Unsolicited'),
(130, 'Valium'),
(131, 'Viagra'),
(132, 'Vicodin'),
(133, 'We hate spam'),
(134, 'Weight loss'),
(135, 'Accept credit cards'),
(136, 'Ad'),
(137, 'All new'),
(138, 'As seen on'),
(139, 'Bargain'),
(140, 'Beneficiary'),
(141, 'Billing'),
(142, 'Bonus'),
(143, 'Cards accepted'),
(144, 'Cash'),
(145, 'Certified'),
(146, 'Cheap'),
(147, 'Claims'),
(148, 'Clearance'),
(149, 'Compare rates'),
(150, 'Credit card offers'),
(151, 'Deal'),
(152, 'Debt'),
(153, 'Discount'),
(154, 'Fantastic'),
(155, 'In accordance with laws'),
(156, 'Income'),
(157, 'Investment'),
(158, 'Join millions'),
(159, 'Lifetime'),
(160, 'Loans'),
(161, 'Luxury'),
(162, 'Marketing solution'),
(163, 'Message contains'),
(164, 'Mortgage rates'),
(165, 'Name brand'),
(166, 'Offer'),
(167, 'Online marketing'),
(168, 'Opt in'),
(169, 'Pre-approved'),
(170, 'Quote'),
(171, 'Rates'),
(172, 'Refinance'),
(173, 'Removal'),
(174, 'Reserves the right'),
(175, 'Score'),
(176, 'Search engine'),
(177, 'Sent in compliance'),
(178, 'Subject to…'),
(179, 'Terms and conditions'),
(180, 'Trial'),
(181, 'Unlimited'),
(182, 'Warranty'),
(183, 'Web traffic'),
(184, 'Work from home'),
(185, 'Xanax'),
(186, 'check if this'),
(187, 'words are there in the query'),
(188, 'if not'),
(189, 'add them'),
(190, 'create a new query');


-- Tracking the Top 10 spam and Ham accounts

CREATE TABLE top_10_spam AS
SELECT email_from AS Top_spam_account,
COUNT(*) AS count_of_spam_mails_send,
MAX(message) AS most_common_message
FROM spam_emails
WHERE is_spam = 1
GROUP BY email_from
ORDER BY count_of_spam_mails_send DESC
LIMIT 10;


CREATE TABLE top_10_ham AS
SELECT email_from AS Top_spam_account, 
COUNT(*) AS count_of_ham_mails_send,
MAX(message) AS most_common_message
FROM ham_emails
WHERE is_spam = 0
GROUP BY email_from
ORDER BY count_of_ham_mails_send DESC
LIMIT 10;

-- Storing the output back to S3 bucket for further analysis

-- SPAM
INSERT OVERWRITE DIRECTORY 's3://ca675-bucket/hive_output/' 
SELECT * FROM top_10_spam ;

-- HAM
INSERT OVERWRITE DIRECTORY 's3://ca675-bucket/hive_output2/' 
SELECT * FROM   top_10_ham ;

