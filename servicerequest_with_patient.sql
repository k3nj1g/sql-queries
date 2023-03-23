SELECT sr.id
  , patient_fio(p.resource) fio
  , p.resource->>'birthDate' "birth-date"
  , identifier_value(p.resource, 'urn:identity:snils:Patient') snils
  , identifier_value(p.resource, 'urn:identity:enp:Patient') enp
--   , jsonb_path_query_array(p.resource, '$.identifier ? (@.system == "urn:identity:enp:Patient").value') all_enp
--   , jsonb_path_query_array(p.resource, '$.identifier ? (@.system == "urn:identity:enp:Patient" && !exists(@.period.end)).value') all_enp
FROM 
(VALUES ('26d90402-23a3-452a-9e33-03fd8c428d30'),
('d31643be-9429-49c0-aadb-f3cb62902fbf'),
('0f1e4046-7889-4a56-9355-9f4e62b3663a'),
('1fa81528-c71b-49ca-b767-a7737b3f7ac5'),
('2461d9f5-de08-49e4-843c-71fef3760ae4'),
('28b3b66d-df22-40a1-8c06-1ed20fbd1977'),
('2908fec2-d39c-4434-8367-0b60c9ee41bb'),
('2c17d857-6e42-4ed0-a0a4-eb08f1cbf950'),
('32b37be2-eb97-4a35-ab60-9ed974faeacd'),
('342b55f5-2f6e-4545-9264-4c383b1dfe51'),
('4122a649-aa24-4a64-ae2c-253a12e5fda5'),
('512481f8-dd80-4cd5-87d6-6c140b0b7fdb'),
('54d2bc04-146a-4c49-8e55-fa369f9fdbd8'),
('5c795c6e-8a7a-4d81-aa40-cca9bd9eb507'),
('5ed8d29d-7722-44ac-9de9-5ef485d73de3'),
('60469714-9926-4923-9088-f8eb8f5b2257'),
('698b4a4a-5e9c-4748-9275-ef1172b02715'),
('6b9e7725-0d91-405a-92cd-f47a4175d04c'),
('7486bf5d-ea3a-4265-a330-3f0c47522159'),
('81cfae02-4402-487a-a364-17e0d27866c5'),
('93d00379-1238-4088-a844-b8b2873b8a7e'),
('95e503c9-3a8e-45d2-8fee-4fd5bfc7b720'),
('974264ad-0d05-4f03-88a6-2e85e2596ed9'),
('9dddf287-795e-4013-a038-b38331d5b832'),
('ad1bda91-27dd-448b-a6c3-4191c07da7e8'),
('aefc3f15-9f86-413e-89fb-c8241ab321b5'),
('b5b91305-0e15-408b-9a3e-d4be8b8f47d0'),
('b6235021-b1fc-4adc-8b1e-a857f82ba099'),
('c0384451-8eeb-4c1b-a93b-3efc4f188567'),
('c66d86fd-2e2c-4294-9024-2fe5cc5b09bf'),
('cbeaf44c-b358-4c47-a7ed-64e4fe5e1dc3'),
('d1040138-42f3-4ce6-bebe-96dfbc612f62'),
('d3f34131-a9cb-4605-a728-b51cf7914b04'),
('da2d5bc2-1e3a-4cdd-8547-1695f8a29dde'),
('dc93dbc9-6f55-449a-b98d-76abc68d0626'),
('edba8d7b-fd0b-47a4-80b4-ec88ba52e548'),
('ee699710-244e-4010-af15-c1c1b292f9e9'),
('fa52b4c6-388d-4674-a9cd-db79cfb5a1c0'),
('0f76990a-bad0-44bc-a07f-94733086a73f'),
('13a08b49-ca7a-4c41-bfc4-2d31642963ab'),
('402742cd-2188-4742-88a9-42da1ded256a'),
('46a2ebca-a026-4b27-af5c-786e570c43d7'),
('59c600dd-288a-47f2-aa7d-ea8d3b9ec40f'),
('a86fc4a0-b85a-4e8b-9a49-f136fc48dd3a'),
('c3d22a37-7284-4f4a-94c6-ea9b2fe35a15'),
('dffd4475-5af4-4ab0-a5b6-d2eb6491eada'),
('02435832-42bd-4420-aba9-5377f0c4eda8'),
('62fcd5f3-7a2b-4242-9403-6562ab7edb6b'),
('64c293f9-37c6-4a52-a288-a90db04520c6'),
('860a7889-74af-472a-9651-c4251bcdcc9d'),
('8e68774b-50ec-4d80-9ae6-b6314f6a3b74'),
('8eca381a-c5db-456f-b4fc-6fb2fb436910'),
('9c6e8055-72e0-4db0-bdef-4af8588ef997'),
('9ce8249a-a096-42a2-9e1b-15ca5d987d62'),
('a8aa5123-68fd-4f10-aab9-1cf24f9f1012'),
('ed6f5a5b-9f5b-4a89-a5de-f6da9376fd9e'),
('1e1f5ad7-2c23-474b-9fce-4baec6c158e1'),
('da4f94c8-7bc6-40cb-9230-5e2e68ad3744'),
('ef6780a2-40e6-4dfb-a131-ca945b9c5152'),
('6ff9f21b-b6a9-4690-ac4c-7e42c2df713f'),
('3c1a0c9a-a8cd-4591-89b0-264029470079'),
('06fbbd3b-00eb-4788-a20c-8345fa3f847d'),
('091c6f52-c1be-4a66-b56a-07b22c8950d6'),
('10300d6d-fc63-4788-989a-20cdbc61a1b6'),
('168a5bdb-3473-4854-b732-d6b414fc7ca0'),
('240fa14d-0207-44c9-b4f1-17d86dd39c47'),
('35390465-d87e-4894-88d2-20920f7bb047'),
('374b466a-c287-4314-b27a-d3459626779f'),
('3d38ed7d-310d-4f6b-8ebe-335aa01f3e9f'),
('3e23f0d5-ddb5-4741-b22c-74a68a1431b1'),
('4e80de2f-2d9e-4517-abb1-978164aacc5b'),
('50882882-3463-4409-b6bb-e136cf3084f2'),
('539e2960-4528-4519-beaf-b5e3fedb202c'),
('5abb8737-044e-4740-a21e-dc92aac063ca'),
('7df15934-dcdc-4e87-9c96-9afe4d26631b'),
('980b23b8-be0b-4c1c-896c-0f5012ffca83'),
('a1351d84-3155-407c-84b8-839ac773b120'),
('a17c036a-507b-4e44-b242-f6cb9e70054d'),
('a49173ab-d4cf-462f-bf63-db618bb47023'),
('b7d167fe-f39b-4749-a46c-a3f428df9588'),
('c2b51547-9c95-4976-b1a2-f446405a792a'),
('c9ccb73c-0bc4-469b-895d-024995d269dd'),
('cf33ac4f-b319-4f14-9e6b-22ff64a7d3cf'),
('dd84e112-db7f-4ba7-a2b7-9e19f0fd1f8d'),
('e2400314-b72a-47aa-b0a4-42b956a03dfc'),
('e725ac47-99b2-4ced-980d-44e4dc7a2d7b'),
('f13265d5-9cf8-4ee6-b34c-6d37d85915d0'),
('ff6c63de-dfe6-4bc8-b69f-dda022f5f6b2'),
('ae90c207-297b-4cfa-bac6-9364535a3131'),
('279ee552-e5a5-4721-8957-5ee3f54f508c'),
('570dec15-995a-40e5-b76f-f372046e949a'),
('36394d91-e952-4f97-87eb-0bab3caba451'),
('6240619d-2f10-45bd-be8c-aaf5edeaf54b'),
('87476385-4548-4b06-abe9-2d45e9e50125'),
('d91bfa09-1012-4271-9a7a-fe59ff0f6960'),
('5f60fde0-046b-405f-9425-d168dcf26662'),
('bfb528cb-23a7-40d4-a4d6-cd2c6d1b1996'),
('e5478c61-8197-4fd6-be49-0c7eb56ebdaf'),
('87e03809-4568-4e55-a974-0286fb1d1490'),
('9bafeb3d-411e-4c51-9784-f7a5bc0782d1'),
('2a24a422-aa6e-45d1-bf59-67e3ceac689c'),
('3a822c18-6e5b-4fc1-9e43-6d9814225793'),
('4e6650e1-87e3-4d9a-959d-88035b1f8f4c'),
('a2366057-08ec-4a8a-b186-18013bdadba3'),
('a879affd-4c7e-4efc-acd7-0407c8f8f18a'),
('07f372b2-0bc6-4946-bf5a-d61d95f72f18'),
('0d859252-945e-4acb-869c-6cebbda1bec1'),
('33b17da1-43b7-49b6-a9e3-28e7c45a0e68'),
('5ab0f8e0-a4cc-488f-af90-99079849f7f9'),
('d1433862-f9fd-4b76-ad10-33065cdf2e29'),
('df8ef37d-0bf1-4ff5-9167-03f944e9c614'),
('e295007b-d7a4-46c1-a1e3-fce94797faa5'),
('ee56d02f-2040-4b73-af29-5da1c8a0ea77'),
('f66d9f4c-4f06-4e8b-9eb3-2c7ba3b91d93'),
('4bb6bdf8-76c2-4411-b9f6-759ba6c373fb'),
('515f20ec-3176-4ad1-ba95-65765d2740ac'),
('58614a0c-7305-4986-b993-3d3f5b78aada'),
('6aeb2134-e887-4322-b45c-4cb1c1fa08fd'),
('705ce3f6-454c-49ca-a4f6-51cea957ce36'),
('95e77982-20b6-4bd0-ade1-b0d4085e5368'),
('b990bab3-5006-4eb7-a1a9-fd351975f349'),
('c9caef76-30ac-4eef-896a-cee8e36e8a56'),
('ddc5138f-d953-467f-9e14-4df246c1210d'),
('e5e792b4-7460-43f7-a5cb-88f9677a617e'),
('0360c641-94ad-4aa7-8ca4-e46225de1c69'),
('0e78c47b-c7a8-4226-8a66-a96043bae122'),
('0f01a27a-6a7e-4423-9ad7-64f6680b8955'),
('6f6c480f-e050-4eef-b13a-865ae9dc8cc3'),
('8ac6740d-0b98-4c3e-b806-02857130eab8'),
('9a497361-b7da-4d63-bff4-52e72ce0f9e4'),
('bc250489-b06b-4f8d-b1d7-728aeb9ff7a8'),
('dcdfefad-dce5-4f5f-80d0-3036279d5caa'),
('e18ac213-bf00-4f03-a586-dc5d886d0dc7'),
('2514fd1c-9939-4947-8b63-dd8ae4ea92a6'),
('7158f49d-b43b-47a6-8efb-cc7a65c2f89e'),
('972bf84f-7832-4274-9f73-11e815d5acee'),
('2599ce43-2e62-4209-904c-e0545f40689e'),
('463cc38e-72f8-4404-9f31-1bbad2fc8ba8'),
('a29c58c5-5bb7-4136-aed5-0079c22dc959'),
('8d2da282-c50f-409b-8b39-123292344939'),
('126311cb-8064-4574-8a80-1419fe62083a'),
('2fd2d434-5586-4720-bf3d-2832a43d34b1'),
('4c9764e9-cdc8-40fb-a39f-8f513357cbee'),
('aacb411f-0ee9-4fcc-b5e0-84d795612dbe'),
('dd22f67b-5702-4f9a-90e8-04df0048749b'),
('008e9cc1-3854-4e81-a267-94e6c903b8d0'),
('02597c73-3d74-4a0b-9ed5-0921e2709a60'),
('23d2558d-d155-46b2-a23c-bb0b4e9d31c0'),
('294fa936-2120-46a6-acc1-a8429b0bdf48'),
('4627898d-9864-4b68-a6b5-9f9c5a584122'),
('81b7ca5e-3aaf-4e67-b005-ab7a56869ee5'),
('8de329af-26c0-4c99-b625-a131f65b38c0'),
('a38961c1-dfd8-41c0-b8e5-d3f9f876b13b'),
('b6b586b9-04c6-4d0e-97a9-2ba6c9531ccb'),
('c210df6e-c5a8-4498-98f8-b1dafb8eacc2'),
('cad308da-fc51-4faf-b516-bf46c7824c04'),
('eff145da-a843-4179-b724-0cd59ad06c18'),
('f6aa2bc8-32e1-424c-b1d4-30cedcce6a2a'),
('6f20eea5-12c0-4893-bb9c-1a932fb99945'),
('2555d370-5c5c-4fee-ab0a-407c4841b1a4'),
('70c275bc-ae68-4bbf-82eb-2773af244ae7'),
('8fea7aea-d834-4a49-b22f-d4c284a13e6b'),
('97f5b5b5-b9e7-43c4-9ad0-201f296555cc'),
('9c7d3379-5828-44b5-94b4-0e3d378fe741'),
('eb70f2af-c034-4056-9db9-bee12d459ab6'),
('f880b6f4-ea5d-4333-b112-82dc398b6b2c'),
('ffd7c454-2a1b-4576-bb45-206ee0948d63'),
('3f595a44-8373-41c9-86ec-1776738d0452'),
('651da284-3ca0-4018-a902-83d2913f4cff'),
('c693d996-5f2c-40cf-a695-4d5a8758ad35'),
('bf56a4bc-bc8b-4519-b085-9bef9cc2dabd'),
('f47a8808-3906-4ba1-8c15-ed86f81adf4d'),
('f5ca5b99-8479-4941-af34-b49a5b88b69e'),
('21f1da38-8e00-4fa4-995a-03a768c23a52'),
('54f95a11-2325-43e4-9b5b-28692fa17965'),
('614627c5-610a-4c39-ba96-5736d5735700'),
('8be34ae9-ef7f-4937-afd2-a358b91f13dc'),
('c34697bb-3683-41d7-840d-95c06d171706'),
('ca568581-27a0-4aba-8ce0-1f5affb3a87b'),
('ddf4d2c2-7ba4-4df3-8083-c4739002ea91'),
('f62b7dd3-cbc9-486a-93b4-130ea76820ca'),
('03f7144b-b02f-41d6-ae0f-12091e160445'),
('1815d8e6-f7ff-402a-b881-f3e8a0af16b2'),
('fed1164f-1397-4381-94e3-87be1fe5d39f'),
('0ef71544-44b0-4708-a353-837db97fa350'),
('122b7876-cd85-43aa-ae13-b227816d0fdf'),
('5c37c386-12ed-4b48-a28c-db760db91429'),
('6e6de1ad-b67f-41b9-a88d-80c39f903ca4'),
('6fd91565-10f9-4ccc-a4e8-05b5ab19832b'),
('792d3bc1-616c-4080-b143-a3b324c04ee9'),
('9496101d-50da-4520-aacc-1b45e59b794f'),
('95b1d36e-59fa-4736-9f68-a11d4897dcb5'),
('95d13434-be39-45f1-a9a7-2a3897fa9401'),
('9bc363b9-0329-4f30-8945-f20a1075d693'),
('aebf1eaa-0d61-423c-a568-df46443f29d9'),
('b35a3513-e26a-4140-b713-cdb3c6e862db'),
('bfb0d583-9ec6-4cd2-a942-10f9be89254d'),
('cecdc7c5-b1cb-43a1-91b4-c7dc74bcdc35'),
('d4a0e7bd-ebe2-402c-8621-8211365df44c'),
('dc5abf8b-3814-4132-bdbb-91c77048fcf3'),
('fc35b7d3-246e-429f-86c7-c0912332843b'),
('295120e5-8b9f-4e3c-910c-8b65a7a51090'),
('fadd0473-0151-4495-a946-59a6d5134880'),
('d9837f24-931c-4690-b391-5b6a83d00eb5'),
('b1c10e27-7c51-4b9f-95f4-8bba554036ae'),
('f9b77b82-b50d-4ddf-a352-94e94296a337'),
('f266388c-1637-49ea-8fca-5b2712fa893c'),
('8ad1bffa-83a7-48ad-b3ff-3f7618f91cca'),
('0863fb2c-3939-44f8-aff3-d03dcc1bac63'),
('1893fc5d-daa6-489c-9a46-e866386d52f7'),
('4c456c06-3255-4dc2-b59c-435da4713861'),
('64124d87-5c71-47b0-b33a-6ebde711a088'),
('87bb2c07-8fe4-4bfb-aad7-adf9a7b5ccd7'),
('a38e2011-d5aa-4697-915c-21a62c05fc6d'),
('a4fb9d3d-29a6-4c43-b648-44ee74af3c27'),
('aa0ae0fb-729e-4c34-8f35-10fa5faf34a6'),
('bdba3065-5782-4f57-88c3-c32d82f4212a'),
('db2f8c8c-36e5-4360-9174-3f92925b14eb'),
('f0f6ce4d-e020-46fd-8196-0136dee74158'),
('fa6d5893-ea0c-40ef-9465-be796c6b7fb2'),
('371482e8-d6f4-4d37-8107-762a216d46ee'),
('71406b36-d69d-43a0-9471-0e6f3e7b6148'),
('7402439c-78cb-428d-816a-11f019b012b5'),
('778bdd84-8254-4242-84fa-035817ceb26c'),
('a1be851d-ee4e-416d-813c-ed41249adfb8'),
('b797ce41-589f-4512-b2ca-af108c848d94'),
('c33d434e-0b03-40c0-a2bf-285616a71c84'),
('d34d7684-02b2-4d48-aadb-ee9f8e8527e2'),
('02b273d5-1088-4ecb-bdde-831347d96abf'),
('1d6e7cc7-2e79-4628-9703-4fdbf20aa1f1'),
('427fce8c-f010-4e01-b5e7-e9149c69e1ca'),
('5125cd30-ea74-4f24-b229-2f18658b8e76'),
('58a06b2f-7ded-4507-b34c-e9e067b84f42'),
('59248df5-50e3-471c-b840-c4d1c3102116'),
('7586c3ca-ee38-49b3-95eb-13de085d3f57'),
('977b0eae-3c56-4ac5-a6cd-cd572ed243dd'),
('5f6ab083-c886-4f81-aec9-0364be791eb3'),
('8f1d7bf4-00e1-4239-9607-ec4609fb9175'),
('e459eaf5-b3bf-4f58-afb4-bd03c9061d20'),
('09051326-2e51-46ca-8467-f8096974f21f'),
('6bf3e7c4-6884-4cc7-93ad-09953212dd5c'),
('74951cb6-e888-40f6-86b4-b539c58e20ed'),
('78b2d12f-d82c-43fd-b070-cea826c8fc8e'),
('a88b3d12-dbba-41f7-8022-c75db28944a3'),
('d5988a2c-b049-47f3-884f-6dbb2072708c'),
('2e954746-45c4-445c-bcf0-8b35a5b5ce7d'),
('52ede4ee-5082-453b-bdc0-641367767f2c'),
('f3a0366e-7688-4d1c-a132-50f0928ad771'),
('13405e3b-b211-4068-9212-e0f3113c503f'),
('3e15d338-e8c9-4602-9d16-ec068f3f3e23'),
('8611a251-f826-403d-965c-172e8fffcebf'),
('e0474832-e9e0-4890-b3ab-c31b5d812042'),
('f1a19be4-64de-40e6-93b3-6f0c7f7ce55d'),
('9d6f93c7-ba13-44ee-b086-5d300d26f083'),
('a72dcece-6a89-4b5d-b95d-c2bf0183d632'),
('e210ba84-f30d-4dd4-a487-c0e6576ccf7c'),
('afd6e6d3-1357-4903-81ba-a6cc4739faf9'),
('e723ee66-6091-4593-82b7-9ef3f7e193c5'),
('6fb7d33e-f510-49a9-aaea-5ae0d662723e'),
('0864ec77-d123-4a28-b8ca-96e97d733466'),
('0f8a9c74-5c2e-47f8-820c-310a07e0296e'),
('250fb579-88e9-4a49-bc2b-9a57723e4bed'),
('5589e188-16fd-4005-8c29-6c382a4633ab'),
('62a00e26-3503-4f25-b672-e6d78f9c7051'),
('6f16aa96-8ff4-4291-9a93-1508f645748e'),
('84e22d3e-bdc4-458e-be38-14eb0f53740b'),
('8b0e7c8d-ecaa-425c-9234-ea09226756d7'),
('8cd5dec0-5439-435c-8aa9-e04406ba69e5'),
('9d3d9de2-de9d-458b-80a0-da93713f4f84'),
('ba4854c7-2b15-45ca-88be-ce134109346b'),
('d82376e8-4a69-4dcb-83ae-0d8e631b53af'),
('f7bae065-6404-4ebe-8b2b-b4b8c06df6a5'),
('f8be6cf0-ddad-4906-a119-bc9c150bb591'),
('fb6caa80-29e6-4fe6-bf69-928dfc121ac0'),
('fe62a165-f5fb-43a7-a4ad-1468a1758216'),
('cd59b6f0-e558-44d8-b0d8-99190008611a'),
('ecd11924-d214-4c5b-b724-09155336419c'),
('24636812-a30e-4470-bab6-7558d3075fee'),
('05789b2e-13b7-47f8-a17c-51463beb271a'),
('3d0a290d-bcc2-4f4d-b16d-59e1251d7e37'),
('81001241-4f6d-4b90-a9ee-11ccbb48e0e1'),
('947b9f9a-ffa5-4d17-8474-16252e2dc1c8'),
('c61cdac5-cf58-4fa4-88fb-fd1560ba41f2'),
('dce03b05-1081-46b0-8bcd-b8cc6aecb629'),
('f0f25bd3-49b6-4232-abbc-27abe3b09d95'),
('ff81b4c0-bc75-4de1-96e4-66b25304ab23'),
('04a4bc11-46fc-46a4-a91a-652e8b1ffd5f'),
('0ce88018-7772-4e34-a97f-a4b01e734699'),
('1305d066-2d8a-438a-a85e-71204d978998'),
('24abe11c-83f3-457b-8c77-3fe6a89ac5f7'),
('2a1c39fe-c37c-47f5-abf7-a1e63d9c8283'),
('332e8753-3ee2-4dc6-aa3d-12f145524dd8'),
('37125bd6-07b3-4384-9b3f-95d0ba37e686'),
('402f99e8-a64e-4291-9a3a-25a5f6151b7a'),
('505c88a6-b0b7-42d0-8c96-5b1ac900f74c'),
('511d8bd7-840a-4c19-9386-8714c0786957'),
('7101a4b7-d18d-4bbe-b8cb-877140719b7e'),
('7ad7a146-7489-4748-9510-24d7fdabe53c'),
('836789ed-7bbc-4a16-a8f8-86917690f59e'),
('8e69aa23-18c1-4c3d-8a0e-ca8437370180'),
('acdc93c1-01f2-4954-a7f6-3d009426f353'),
('dde67fc9-7828-4fcf-a527-1768cf5b5215'),
('f042ca8d-bdea-466e-8b91-ba9ade1100b9'),
('f410fec7-96be-4cf6-a152-61eaecef3971'),
('fe9a8301-8053-4fad-b9b6-cb7fd6887c59'),
('fff799f8-9512-4f43-8c31-cf2d666b2a2b'),
('21acd91e-d5ce-4fd9-85df-ce50fe6bc7f4'),
('3b0529aa-0734-4625-ac37-3bed1fbeaf8c'),
('6a9df156-b6e8-43e4-a44c-4a4f646a4e42'),
('99060432-0121-4e0d-a308-275ee674a959'),
('a56f90fe-2681-4504-a308-e002a6715866'),
('af7b6db6-4248-4cac-8101-9457d3e154aa'),
('10fe4383-1c7c-47c9-965b-613e53090f8a'),
('11b014cd-9440-49f7-9aa3-160e0e72acf1'),
('4ff74af0-0851-4cf2-ae0c-61bcf5d82f0f'),
('514c610e-14aa-4fad-b588-db881957938a'),
('59ca57ea-c692-4034-9b1c-f74bf2be83b5'),
('5d88d09c-f220-4451-873f-8952de529336'),
('64eeaf65-18cd-4962-8c44-2d0df31ac72d'),
('7a5dcf0a-abae-428c-9bcc-16e045127848'),
('9cd3baf2-5312-4cb5-9f46-70577b2d1f97'),
('ab416a28-09c1-4470-b731-f58f01753da5'),
('c0e92a18-1c2e-4b7f-8e8d-67a527e45727'),
('d894884a-9cd6-4891-bfb7-ff9fcdb45559'),
('04fc8efb-1d1a-4543-8aee-6a882a62a19d'),
('33cf1486-5b62-4619-b1aa-e8c165fd6401'),
('4217e369-d376-42d7-bcd8-8588453f36fb'),
('b1860ec5-bc3d-4e46-bc89-b26c4f474c6a'),
('e608e0f5-57d1-4c3a-86cf-261255c103cf'),
('eca610eb-3604-4e43-bc7f-bf50ee615a8a')) v(sr_id)
JOIN servicerequest sr
  on sr.id = v.sr_id
left join patient p on 
  (p.resource @@ logic_include(sr.resource, 'subject') 
  OR p.id = any(array(SELECT jsonb_path_query(sr.resource, '$.subject[*].id') #>> '{}')))
  and coalesce(p.resource->>'active', 'true') = 'true'
